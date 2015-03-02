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
import logging
import sys

from thunder.streaming.feeder.core import build_filecheck_generators, runloop
from thunder.streaming.feeder.feeders import CopyAndMoveFeeder

from thunder.streaming.feeder.utils.logger import global_logger
from thunder.streaming.feeder.utils.regex import RegexMatchToPredicate


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
    parser.add_option("--filter-regex-file", default=None,
                      help="File containing python regular expression. If passed, only move files for which " +
                           "the base filename matches the given regex.")
    opts, args = parser.parse_args()

    if len(args) != 2:
        print >> sys.stderr, parser.get_usage()
        sys.exit(1)

    setattr(opts, "indir", args[0])
    setattr(opts, "outdir", args[1])

    return opts


def main():
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(logging.Formatter('%(levelname)s:%(name)s:%(asctime)s:%(message)s'))
    global_logger.get().addHandler(_handler)
    global_logger.get().setLevel(logging.INFO)

    opts = parse_options()

    if opts.filter_regex_file:
        pred_fcn = RegexMatchToPredicate.fromFile(opts.filter_regex_file).predicate
    else:
        pred_fcn = None

    feeder = CopyAndMoveFeeder.fromOptions(opts)
    file_checkers = build_filecheck_generators(opts.indir, opts.mod_buffer_time,
                                               max_files=opts.max_files, filename_predicate=pred_fcn)
    runloop(file_checkers, feeder, opts.poll_time)

if __name__ == "__main__":
    main()