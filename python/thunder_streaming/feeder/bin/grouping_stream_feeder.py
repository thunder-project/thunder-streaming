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
import sys

from thunder_streaming.feeder.utils.logger import global_logger
from thunder_streaming.feeder.core import build_filecheck_generators, runloop, get_parsing_functions
from thunder_streaming.feeder.feeders import SyncCopyAndMoveFeeder


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
    parser.add_option("--max-files", type="int", default=-1,
                      help="Max files to copy in one iteration "
                           "(negative disables), default %default")
    parser.add_option("--imgprefix", default="img")
    parser.add_option("--behavprefix", default="behav")
    parser.add_option("--prefix-regex-file", default=None)
    parser.add_option("--timepoint-regex-file", default=None)
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
    global_logger.get().addHandler(_handler)
    global_logger.get().setLevel(logging.INFO)

    opts = parse_options()

    fname_to_qname_fcn, fname_to_timepoint_fcn = get_parsing_functions(opts)
    feeder = SyncCopyAndMoveFeeder(opts.outdir, opts.linger_time, (opts.imgprefix, opts.behavprefix),
                                   fname_to_qname_fcn=fname_to_qname_fcn,
                                   fname_to_timepoint_fcn=fname_to_timepoint_fcn)

    file_checkers = build_filecheck_generators((opts.imgdatadir, opts.behavdatadir), opts.mod_buffer_time,
                                               max_files=opts.max_files,
                                               filename_predicate=fname_to_qname_fcn)
    runloop(file_checkers, feeder, opts.poll_time)

if __name__ == "__main__":
    main()