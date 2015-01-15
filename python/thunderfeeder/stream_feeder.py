#!/usr/bin/env python
"""Basic example of feeding files into a directory being watched by spark streaming.

This script will monitor a directory tree passed as a argument. When new files are added inside this tree
(provided that they are lexicographically later than the last file fed into the stream - see updating_walk.py)
they will be first copied into a temporary directory, then moved into the passed output directory. After a specified
lag time, they will be automatically deleted from the output directory.

The temporary directory must be on the same filesystem as the output directory, or else errors will likely be
thrown by os.rename. The root of the temporary directory tree can be set on most linux systems by the TMP environment
variable.
"""
import errno
import logging
import os
import shutil
import sys
import tempfile
import time

from updating_walk import updating_walk as uw


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


def parse_options():
    import optparse
    parser = optparse.OptionParser(usage="%prog indir outdir [options]")
    parser.add_option("-p", "--poll-time", type="float", default=1.0,
                      help="Time between checks of indir in s, default %default")
    parser.add_option("-m", "--mod-buffer-time", type="float", default=1.0,
                      help="Time to wait after last file modification time before feeding file into stream, "
                           "default %default")
    parser.add_option("-l", "--linger-time", type="float", default=5.0,
                      help="Time to wait after feeding into stream before deleting intermediate file "
                           "(negative time disables), default %default")
    parser.add_option("--max-files", type="int", default=-1,
                      help="Max files to copy in one iteration "
                           "(negative disables), default %default")
    opts, args = parser.parse_args()

    if len(args) != 2:
        print >> sys.stderr, parser.get_usage()
        sys.exit(1)

    setattr(opts, "indir", args[0])
    setattr(opts, "outdir", args[1])

    return opts


def file_check_generator(source_dir, mod_buffer_time, max_files=-1):
    """Generator function that polls the passed directory tree for new files, using the updating_walk.py logic.

    This generator will restart the underlying updating_walk at the last seen file if the updating walk runs
    out of available files.
    """
    next_batch_file, walker_restart_file = None, None
    walker = uw(source_dir)
    while True:
        filebatch = []
        files_left = max_files
        try:
            if not next_batch_file:
                next_batch_file = next(walker)
                walker_restart_file = next_batch_file

            delta = time.time() - os.stat(next_batch_file).st_mtime
            while delta > mod_buffer_time and files_left:
                filebatch.append(next_batch_file)
                files_left -= 1
                next_batch_file = None  # reset in case of exception on next line
                next_batch_file = next(walker)
                walker_restart_file = next_batch_file

        except StopIteration:
            # no files left, restart after polling interval
            if not filebatch:
                _logger.get().info("Out of files, waiting...")
            walker = uw(source_dir, walker_restart_file)
        yield filebatch


def runloop(source_dir_or_dirs, feeder, poll_time, mod_buffer_time, max_files=-1):
    """ Main program loop. This will check for new files in the passed input directories using file_check_generator,
    push any new files found into the passed Feeder subclass via its feed() method, wait for poll_time,
    and repeat forever.
    """
    if isinstance(source_dir_or_dirs, basestring):
        source_dirs = [source_dir_or_dirs]
    else:
        source_dirs = source_dir_or_dirs

    last_time = time.time()
    file_checkers = [file_check_generator(source_dir, mod_buffer_time, max_files=max_files)
                     for source_dir in source_dirs]

    while True:
        for file_checker in file_checkers:
            # this should never throw StopIteration, will just yield an empty list if nothing is avail:
            filebatch = feeder.feed(next(file_checker))
            if filebatch:
                _logger.get().info("Pushed %d files, last: %s", len(filebatch), os.path.basename(filebatch[-1]))

        removedfiles = feeder.clean()
        if removedfiles:
            _logger.get().info("Removed %d temp files, last: %s", len(removedfiles), os.path.basename(removedfiles[-1]))

        next_time = last_time + poll_time
        try:
            time.sleep(next_time - time.time())
        except IOError, e:
            if e.errno == errno.EINVAL:
                # passed a negative number, which is fine, just don't sleep
                pass
            else:
                raise e
        last_time = next_time


def main():
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(logging.Formatter('%(levelname)s:%(name)s:%(asctime)s:%(message)s'))
    _logger.get().addHandler(_handler)
    _logger.get().setLevel(logging.INFO)

    opts = parse_options()

    feeder = CopyAndMoveFeeder.fromOptions(opts)
    runloop(opts.indir, feeder, opts.poll_time, opts.mod_buffer_time, max_files=opts.max_files)


class StreamFeederLogger(object):

    def __init__(self, name):
        self._name = name
        self._logger = None

    def get(self):
        if not self._logger:
            self._logger = logging.getLogger(self._name)
        return self._logger


_logger = StreamFeederLogger("streamfeeder")

if __name__ == "__main__":
    main()