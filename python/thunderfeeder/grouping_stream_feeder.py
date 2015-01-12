#!/usr/bin/env python
"""An elaboration on stream_feeder that watches for pairs of files with matching suffixes. Only when a matching
pair is found will both files be moved into the output directory.

When run as a script, this file expects to find matching files in two separate directory trees. These files are
assumed to represent imaging and behavioral data from the same point in time.

Files are matched based on having identical suffixes after the first appearance of a delimiter character '_', excluding
filename extensions. So 'foo_abc.txt' and 'bar_abc' match, but 'foo_123' and 'bar_124' do not.

Note that this script will block forever waiting for a match. So for instance given files a_01, a_02, a_03, b_01, and
b_03, after moving the a_01 b_01 pair it will block waiting for a b_02 to show up.

"""

import logging
import os
import sys

from collections import deque
from itertools import imap, izip, groupby, tee
from itertools import product as iproduct
from operator import itemgetter

from stream_feeder import runloop, _logger, CopyAndMoveFeeder


def unique_justseen(iterable, key=None):
    """List unique elements, preserving order. Remember only the element just seen.
    Taken from python itertools recipes.
    """
    # unique_justseen('AAAABBBCCDAABBB') --> A B C D A B
    # unique_justseen('ABBCcAD', str.lower) --> A B C A D
    return imap(next, imap(itemgetter(1), groupby(iterable, key)))

# next two functions from stackoverflow user hughdbrown:
# http://stackoverflow.com/questions/3755136/pythonic-way-to-check-if-a-list-is-sorted-or-not/4404056#4404056
def pairwise(iterable):
    a, b = tee(iterable)
    next(b, None)
    return izip(a, b)

# tests for strict ordering, will be false for dups
def is_sorted(iterable, key=lambda a, b: a < b):
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
    def __init__(self, feeder_dir, linger_time, prefixes, prefix_delim='_'):
        super(SyncCopyAndMoveFeeder, self).__init__(feeder_dir=feeder_dir, linger_time=linger_time)
        self.prefix_delim = prefix_delim
        self.file_prefix_to_queue = {}
        for prefix in prefixes:
            self.file_prefix_to_queue[prefix] = deque()
        self.keys_to_fullnames = {}

    @staticmethod
    def getFilenamePrefix(filename, delim):
        return SyncCopyAndMoveFeeder.getFilenamePrefixAndPostfix(filename, delim)[0]

    @staticmethod
    def getFilenamePostfix(filename, delim):
        return SyncCopyAndMoveFeeder.getFilenamePrefixAndPostfix(filename, delim)[1]

    @staticmethod
    def getFilenamePrefixAndPostfix(filename, delim):
        bname = os.path.splitext(os.path.basename(filename))[0]
        splits = bname.split(delim, 1)
        prefix = splits[0]
        postfix = splits[1] if len(splits) > 1 else ''
        return prefix, postfix

    def get_matching_first_entry(self):
        """Pops and returns the first entry across all queues if the first entry
        is the same on all queues, otherwise return None and leave queues unchanged
        """
        matched = None
        try:
            for queue in self.file_prefix_to_queue.itervalues():
                first = queue[0]
                if matched:
                    if not first == matched:
                        matched = None
                        break
                else:
                    matched = first
        except IndexError:
            # don't have anything in at least one queue
            matched = None

        if matched is not None:
            for queue in self.file_prefix_to_queue.itervalues():
                queue.popleft()
        return matched

    def match_filenames(self, filenames):
        """Update internal queues with passed filenames. Returns names that match across the head of all queues if
        any are found, or an empty list otherwise.
        """
        # insert
        # we assume that usually we'll just be appending to the end - other options
        # include heapq and bisect, but it probably doesn't really matter
        for filename in filenames:
            prefix, postfix = SyncCopyAndMoveFeeder.getFilenamePrefixAndPostfix(filename, self.prefix_delim)
            self.file_prefix_to_queue[prefix].append(postfix)
            self.keys_to_fullnames[(prefix, postfix)] = filename

        # maintain sorting and dedup:
        for prefix, queue in self.file_prefix_to_queue.iteritems():
            if not is_sorted(queue):
                self.file_prefix_to_queue[prefix] = deque(unique_justseen(sorted(list(queue))))

        # all queues are now sorted and unique-ified

        # check for matching first entries across queues
        matching = self.get_matching_first_entry()
        matches = []
        # TODO: add max # matches here
        while matching:
            matches.append(matching)
            matching = self.get_matching_first_entry()

        # convert matches back to full filenames
        fullnamekeys = list(iproduct(self.file_prefix_to_queue.iterkeys(), matches))
        fullnames = [self.keys_to_fullnames.pop(key) for key in fullnamekeys]
        fullnames.sort()
        return fullnames

    def feed(self, filenames):
        fullnames = self.match_filenames(filenames)
        return super(SyncCopyAndMoveFeeder, self).feed(fullnames)


def parse_options():
    import optparse
    parser = optparse.OptionParser(usage="%prog imgdatadir behavdatadir outdir [options]")
    parser.add_option("-p", "--poll-time", type="float", default=1.0,
                      help="Time between checks of datadir in s, default %default")
    parser.add_option("-m", "--mod-buffer-time", type="float", default=1.0,
                      help="Time to wait after last file modification time before feeding file into stream, "
                           "default %default")
    parser.add_option("-l", "--linger-time", type="float", default=5.0,
                      help="Time to wait after feeding into stream before deleting intermediate file "
                           "(negative time disables), default %default")
    parser.add_option("--imgprefix", default="img")
    parser.add_option("--behavprefix", default="behav")
    opts, args = parser.parse_args()

    if len(args) != 3:
        print >> sys.stderr, parser.get_usage()
        sys.exit(1)

    setattr(opts, "imgdatadir", args[0])
    setattr(opts, "behavdatadir", args[1])
    setattr(opts, "outdir", args[2])

    return opts


def main():
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(logging.Formatter('%(levelname)s:%(name)s:%(asctime)s:%(message)s'))
    _logger.get().addHandler(_handler)
    _logger.get().setLevel(logging.INFO)

    opts = parse_options()

    feeder = SyncCopyAndMoveFeeder(opts.outdir, opts.linger_time, (opts.imgprefix, opts.behavprefix))

    runloop((opts.imgdatadir, opts.behavdatadir), feeder, opts.poll_time, opts.mod_buffer_time)

if __name__ == "__main__":
    main()