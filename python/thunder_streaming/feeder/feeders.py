"""Feeder and subclasses, which abstract a queue or queues of files.
"""
from collections import deque
from itertools import imap, groupby, tee, izip
from itertools import product as iproduct
import numpy as np
from operator import itemgetter
import os
import shutil
import tempfile
import time

from thunder.streaming.feeder.transpose import transpose_files, transpose_files_to_series, \
    transpose_files_to_linear_series
from thunder.streaming.feeder.utils.filenames import getFilenamePostfix, getFilenamePrefix
from thunder.streaming.feeder.utils.logger import global_logger


class Feeder(object):
    """Superclass for objects that take in a set of filenames and push the corresponding files out
    to a consumer.
    """
    def feed(self, filenames):
        """Abstract method that when called, pushes the passed files out to a consumer.

        Implementations should return a list of the filenames that have been successfully pushed out
        to the consumer. This may be a subset of those passed in the feed() call.
        """
        raise NotImplementedError

    def clean(self):
        """Performs any required cleanup, such as deleting copied files.

        Implementations should ensure that any cleanup actions are safe to perform (e.g. the ultimate
        consumer has already consumed the files). If there are no safe cleanup actions, this method should
        return. Future calls may result in cleanup actions being performed.

        Implementations should return a list of filenames that are deleted by the clean() action.

        This implementation does nothing.
        """
        return []


class LastModifiedCleaner(Feeder):
    """Abstract subclass of Feeder that provides a "delete after delay" clean() method.
    """
    def __init__(self, feeder_dir, linger_time):
        """
        Specifies a directory and a delay time after which files found in the directory are to be deleted.

        The delay time is measured from the file's last modification time.

        Parameters
        ----------
        feeder_dir: string
            Path to directory from which files are to be deleted.
        linger_time: float
            Time in seconds.
        :return:
        """
        self.feeder_dir = str(feeder_dir)
        self.linger_time = float(linger_time)

        if not os.path.isdir(feeder_dir):
            raise ValueError("Feeder directory must be an existing directory path; got '%s'" % self.feeder_dir)

    def clean(self):
        """Deletes files found in self.feeder_dir whose last modified time is longer ago than self.linger_time.
        """
        if self.linger_time < 0:
            return []
        now = time.time()
        removed = []
        for fname in os.listdir(self.feeder_dir):
            absname = os.path.join(self.feeder_dir, fname)
            if os.path.isfile(absname) and now - os.stat(absname).st_mtime > self.linger_time:
                os.remove(absname)
                removed.append(fname)
        removed.sort()
        return removed


class CopyAndMoveFeeder(LastModifiedCleaner):
    """Concrete feeder implementation that copies files into the specified output directory.

    This first copies files into a temporary directory, which should be on the same filesystem
    as the ultimate output directory. (This can be controlled by the TMP environment var - see documentation
    in the python tempfile module.) After this copy, the files are moved into the final output directory
    by an os.rename() call, which advertises that it will be an atomic operation for files on the same filesystem.
    This rename() call may throw an exception if the temp directory is on a different filesystem.
    """
    @classmethod
    def fromOptions(cls, opts):
        return cls(opts.outdir, opts.linger_time)

    def feed(self, filenames):
        copydir = tempfile.mkdtemp()
        try:
            basenames = []
            for fname in filenames:
                bname = os.path.basename(fname)
                shutil.copyfile(fname, os.path.join(copydir, bname))
                basenames.append(bname)
            for fname in basenames:
                srcname = os.path.join(copydir, fname)
                # touch prior to atomic move operation to delay slurping by spark
                os.utime(srcname, None)
                os.rename(srcname, os.path.join(self.feeder_dir, fname))
        finally:
            shutil.rmtree(copydir)
        return filenames


def unique_justseen(iterable, key=None):
    """List unique elements, preserving order. Remember only the element just seen.
    Taken from python itertools recipes.
    """
    # unique_justseen('AAAABBBCCDAABBB') --> A B C D A B
    # unique_justseen('ABBCcAD', str.lower) --> A B C A D
    return imap(next, imap(itemgetter(1), groupby(iterable, key)))


def pairwise(iterable):
    # this and is_sorted from stackoverflow user hughdbrown:
    # http://stackoverflow.com/questions/3755136/pythonic-way-to-check-if-a-list-is-sorted-or-not/4404056#4404056
    a, b = tee(iterable)
    next(b, None)
    return izip(a, b)


def is_sorted(iterable, key=lambda a, b: a < b):
    # tests for strict ordering, will be false for dups
    return all(key(a, b) for a, b in pairwise(iterable))


class SyncCopyAndMoveFeeder(CopyAndMoveFeeder):
    """This feeder will wait for matching pairs of files, as described in the module docstring,
    before copying the pair into the passed output directory. Its behavior is otherwise the
    same as CopyAndMoveFeeder.

    Filenames that are not immediately matched on a first call to feed() are stored in internal queues,
    to be checked on the next feed() call. The internal queues are sorted alphabetically by file name, and
    at each feed() call only the head of the queue is checked for a possible match. This can lead to
    waiting forever for a match for one particular file, as described in the module docstring.
    """
    def __init__(self, feeder_dir, linger_time, qnames,
                 fname_to_qname_fcn=getFilenamePrefix,
                 fname_to_timepoint_fcn=getFilenamePostfix,
                 check_file_size_mismatch=False,
                 check_skip_in_sequence=True):
        super(SyncCopyAndMoveFeeder, self).__init__(feeder_dir=feeder_dir, linger_time=linger_time)
        self.qname_to_queue = {}
        for qname in qnames:
            self.qname_to_queue[qname] = deque()
        self.keys_to_fullnames = {}
        self.fname_to_qname_fcn = fname_to_qname_fcn
        self.fname_to_timepoint_fcn = fname_to_timepoint_fcn
        self.qname_to_expected_size = {} if check_file_size_mismatch else None
        self.do_check_sequence = check_skip_in_sequence
        self.last_timepoint = None
        self.last_mismatch = None
        self.last_mismatch_time = None
        # time in s to wait after detecting a mismatch before popping mismatching elements:
        self.mismatch_wait_time = 5.0

    def check_and_pop_mismatches(self, first_elts):
        """Checks for a mismatched first elements across queues.

        If the first mismatched element has remained the same for longer than
        self.mismatch_wait_time, then start popping out mismatching elements.

        Updates self.last_mismatch and self.last_mismatch_time
        """
        comp_elt = first_elts[0]
        # the below list comprehension Does The Right Thing for first_elts of length 1
        # and returns [], and all([]) is True.
        if comp_elt is not None and all([elt == comp_elt for elt in first_elts[1:]]):
            matched = comp_elt
            self.last_mismatch = None
            self.last_mismatch_time = None
        else:
            matched = None
            # this returns None if there are any Nones in the list:
            cur_mismatch = reduce(min, first_elts)
            if cur_mismatch is None:
                # if there is at least one None, then there is some empty queue
                # we don't consider it a mismatch unless there are first elements in each queue, and
                # they don't match - so if a queue is empty, we don't have a mismatch
                self.last_mismatch = None
                self.last_mismatch_time = None
            else:
                now = time.time()
                if self.last_mismatch:  # we already had a mismatch last time through, presumably on the same elt
                    if self.last_mismatch != cur_mismatch:
                        # blow up
                        raise Exception("Current mismatch '%s' doesn't match last mismatch '%s' " %
                                        (cur_mismatch, self.last_mismatch) + "- this shouldn't happen")
                    if now - self.last_mismatch_time > self.mismatch_wait_time:
                        # we have been stuck on this element for longer than mismatch_wait_time
                        # find the next-lowest element - this is not None, since the other queues are not empty
                        next_elts = first_elts[:]  # copy
                        next_elts.remove(cur_mismatch)
                        next_elt = reduce(min, next_elts)
                        # cycle through *all* queues, removing any elts less than next_elt
                        popping = True
                        while popping:
                            popping = False
                            for qname, q in self.qname_to_queue.iteritems():
                                if q and q[0] < next_elt:
                                    discard = q.popleft()
                                    popping = True
                                    global_logger.get().warn("Discarding item '%s' from queue '%s'; " % (discard, qname) +
                                                       "waited for match for more than %g s" % self.mismatch_wait_time)
                        # finished popping all mismatching elements less than next_elt
                        # we might have a match at this point, but wait for next iteration to pick up
                        self.last_mismatch = None
                        self.last_mismatch_time = None
                else:
                    self.last_mismatch = cur_mismatch
                    self.last_mismatch_time = now
        return matched

    def get_matching_first_entry(self):
        """Pops and returns the first entry across all queues if the first entry
        is the same on all queues, otherwise return None and leave queues unchanged
        """
        first_elts = []
        for q in self.qname_to_queue.itervalues():
            first_elts.append(q[0] if q else None)

        matched = self.check_and_pop_mismatches(first_elts)

        if matched is not None:
            for queue in self.qname_to_queue.itervalues():
                queue.popleft()
        return matched

    def filter_size_mismatch_files(self, filenames):
        filtered_timepoints = []
        for filename in filenames:
            size = os.path.getsize(filename)
            bname = os.path.basename(filename)
            queuename = self.fname_to_qname_fcn(bname)
            timepoint = self.fname_to_timepoint_fcn(bname)
            expected_size = self.qname_to_expected_size.setdefault(queuename, size)
            if size != expected_size:
                filtered_timepoints.append(timepoint)
                global_logger.get().warn(
                    "Size mismatch on '%s', discarding timepoint '%s'. (Expected %d bytes, got %d bytes.)",
                    filename, timepoint, expected_size, size)
        if filtered_timepoints:
            return [filename for filename in filenames if
                    self.fname_to_timepoint_fcn(os.path.basename(filename)) not in filtered_timepoints]
        else:
            return filenames

    def check_sequence(self, timepoint_string):
        if self.last_timepoint is None:
            self.last_timepoint = int(timepoint_string)
            return
        cur_timepoint = int(timepoint_string)
        if cur_timepoint != self.last_timepoint + 1:
            global_logger.get().warn("Missing timepoints detected, went from '%d' to '%d'",
                               self.last_timepoint, cur_timepoint)
        self.last_timepoint = cur_timepoint

    def match_filenames(self, filenames):
        """Update internal queues with passed filenames. Returns names that match across the head of all queues if
        any are found, or an empty list otherwise.
        """
        # insert
        # we assume that usually we'll just be appending to the end - other options
        # include heapq and bisect, but it probably doesn't really matter
        for filename in filenames:
            qname = self.fname_to_qname_fcn(filename)
            if qname is None:
                global_logger.get().warn("Could not get queue name for file '%s', skipping" % filename)
                continue
            tpname = self.fname_to_timepoint_fcn(filename)
            if tpname is None:
                global_logger.get().warn("Could not get timepoint for file '%s', skipping" % filename)
                continue
            self.qname_to_queue[qname].append(tpname)
            self.keys_to_fullnames[(qname, tpname)] = filename

        # maintain sorting and dedup:
        for qname, queue in self.qname_to_queue.iteritems():
            if not is_sorted(queue):
                self.qname_to_queue[qname] = deque(unique_justseen(sorted(list(queue))))

        # all queues are now sorted and unique-ified

        # check for matching first entries across queues
        matching = self.get_matching_first_entry()
        matches = []
        dcs = self.do_check_sequence
        while matching:
            if dcs:
                self.check_sequence(matching)
            matches.append(matching)
            matching = self.get_matching_first_entry()

        # convert matches back to full filenames
        fullnamekeys = list(iproduct(self.qname_to_queue.iterkeys(), matches))
        fullnames = [self.keys_to_fullnames.pop(key) for key in fullnamekeys]
        fullnames.sort()

        # filter out files that are smaller than the first file to be added to the queue, if requested
        # this attempts to check for and work around an error state where some files are incompletely
        # transferred
        if self.qname_to_expected_size is not None:
            fullnames = self.filter_size_mismatch_files(fullnames)

        return fullnames

    def feed(self, filenames):
        fullnames = self.match_filenames(filenames)
        return super(SyncCopyAndMoveFeeder, self).feed(fullnames)


class SyncSeriesFeeder(SyncCopyAndMoveFeeder):
    """A Feeder implementation that looks for matching pairs of files, as in SyncCopyAndMoveFeeder, and
    them writes out these matching pairs as a single Series binary file.

    Expected file prefixes must be given at object construction. The Series data will be written out
    in the order given by this prefixes argument - so for instance in order to write out behavioral data
    after imaging data in the Series binary file output, prefixes should be specified as (imagefileprefix,
    behaviorfileprefix), and not the other way around.

    If a shape tuple is given at construction, then the output will have valid subscript indices according
    to this expected shape. See transpose_files() (no shape passed) and transpose_files_to_series() (with shape).
    """
    def __init__(self, feeder_dir, linger_time, prefixes, shape=None, linear=False, dtype='uint16', indtype='uint16',
                 fname_to_qname_fcn=getFilenamePrefix, fname_to_timepoint_fcn=getFilenamePostfix,
                 check_file_size=False, check_skip_in_sequence=True):
        super(SyncSeriesFeeder, self).__init__(feeder_dir, linger_time, prefixes,
                                               fname_to_qname_fcn=fname_to_qname_fcn,
                                               fname_to_timepoint_fcn=fname_to_timepoint_fcn,
                                               check_file_size_mismatch=check_file_size,
                                               check_skip_in_sequence=check_skip_in_sequence)
        self.prefixes = list(prefixes)
        self.shape = shape
        self.linear = linear
        self.dtype = dtype
        self.indtype = indtype

    def get_series_filename(self, srcfilenames, bytesize):
        startcount = self.fname_to_timepoint_fcn(os.path.basename(srcfilenames[0]))
        endcount = self.fname_to_timepoint_fcn(os.path.basename(srcfilenames[-1]))
        return "series-%s-%s_bytes%d.bin" % (startcount, endcount, bytesize)

    def feed(self, filenames):
        fullnames = self.match_filenames(filenames)

        if fullnames:
            tmpfd, tmpfname = tempfile.mkstemp()
            tmpfp = os.fdopen(tmpfd, 'w')
            try:
                nindices_written = 0
                ninput_files = 0
                for prefix in self.prefixes:
                    curnames = [fn for fn in fullnames if self.fname_to_qname_fcn(fn) == prefix]
                    curnames.sort()
                    ninput_files = len(curnames)  # should be same for all prefixes
                    if (not self.linear) and (self.shape is None):
                        nindices_written += transpose_files(curnames, tmpfp, dtype=self.dtype)
                    elif self.linear:
                        nindices_written += transpose_files_to_linear_series(curnames, tmpfp,
                                                                             dtype=self.dtype, indtype=self.indtype,
                                                                             startlinidx=nindices_written)
                    else:
                        nindices_written += transpose_files_to_series(curnames, tmpfp, tuple(self.shape),
                                                                      dtype=self.dtype, indtype=self.indtype,
                                                                      startlinidx=nindices_written)
                tmpfp.close()

                record_vals_size = ninput_files * np.dtype(self.dtype).itemsize
                if self.linear:
                    recordsize = np.dtype(self.dtype).itemsize + record_vals_size
                elif self.shape:
                    recordsize = len(self.shape)*2 + record_vals_size  # key size in bytes + values size in bytes
                else:
                    recordsize = record_vals_size
                newname = self.get_series_filename(fullnames, recordsize)

                # touch prior to atomic move operation to delay slurping by spark
                os.utime(tmpfname, None)
                os.rename(tmpfname, os.path.join(self.feeder_dir, newname))
            finally:
                if not tmpfp.closed:
                    tmpfp.close()
                if os.path.isfile(tmpfname):
                    os.remove(tmpfname)
        return fullnames