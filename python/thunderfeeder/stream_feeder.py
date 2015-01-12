#!/usr/bin/env python
import errno
import logging
import os
import shutil
import sys
import tempfile
import time

from updating_walk import updating_walk as uw


class Feeder(object):
    def feed(self, filenames):
        raise NotImplementedError

    def clean(self):
        pass


class LastModifiedCleaner(Feeder):
    def __init__(self, feeder_dir, linger_time):
        self.feeder_dir = str(feeder_dir)
        self.linger_time = float(linger_time)

        if not os.path.isdir(feeder_dir):
            raise ValueError("Feeder directory must be an existing directory path; got '%s'" % self.feeder_dir)

    def clean(self):
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
    opts, args = parser.parse_args()

    if len(args) != 2:
        print >> sys.stderr, parser.get_usage()
        sys.exit(1)

    setattr(opts, "indir", args[0])
    setattr(opts, "outdir", args[1])

    return opts


def file_check_generator(source_dir, mod_buffer_time):
    next_batch_file, walker_restart_file = None, None
    walker = uw(source_dir)
    while True:
        filebatch = []
        try:
            if not next_batch_file:
                next_batch_file = next(walker)
                walker_restart_file = next_batch_file

            delta = time.time() - os.stat(next_batch_file).st_mtime
            while delta > mod_buffer_time:
                filebatch.append(next_batch_file)
                next_batch_file = None  # reset in case of exception on next line
                next_batch_file = next(walker)
                walker_restart_file = next_batch_file

        except StopIteration:
            # no files left, restart after polling interval
            _logger.get().info("Out of files, waiting...")
            walker = uw(source_dir, walker_restart_file)
        yield filebatch


def runloop(source_dir_or_dirs, feeder, poll_time, mod_buffer_time):

    if isinstance(source_dir_or_dirs, basestring):
        source_dirs = [source_dir_or_dirs]
    else:
        source_dirs = source_dir_or_dirs

    last_time = time.time()
    file_checkers = [file_check_generator(source_dir, mod_buffer_time) for source_dir in source_dirs]

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
    runloop(opts.indir, feeder, opts.poll_time, opts.mod_buffer_time)


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