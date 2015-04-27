#!/usr/bin/env python
"""A variant of grouping_series_stream_feeder, this script watches only a single directory for new image files,
which it then converts into the Thunder series binary format and copies into the Spark input directory.
"""
import logging
import sys

from thunder_streaming.feeder.core import build_filecheck_generators, runloop
from thunder_streaming.feeder.utils.logger import global_logger
from grouping_series_stream_feeder import SyncSeriesFeeder, get_parsing_functions


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
    parser.add_option("--shape", type="int", default=None, nargs=3)
    parser.add_option("--linear", action="store_true", default=False)
    parser.add_option("--dtype", default="uint16")
    parser.add_option("--indtype", default="uint16")
    parser.add_option("--prefix-regex-file", default=None)
    parser.add_option("--timepoint-regex-file", default=None)
    opts, args = parser.parse_args()

    if len(args) != 2:
        print >> sys.stderr, parser.get_usage()
        sys.exit(1)

    setattr(opts, "imgdatadir", args[0])
    setattr(opts, "outdir", args[1])

    return opts


def main():
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(logging.Formatter('%(levelname)s:%(name)s:%(asctime)s:%(message)s'))
    global_logger.get().addHandler(_handler)
    global_logger.get().setLevel(logging.INFO)

    opts = parse_options()

    fname_to_qname_fcn, fname_to_timepoint_fcn = get_parsing_functions(opts)
    feeder = SyncSeriesFeeder(opts.outdir, opts.linger_time, (opts.imgprefix,),
                              shape=opts.shape, dtype=opts.dtype, linear=opts.linear, indtype=opts.indtype,
                              fname_to_qname_fcn=fname_to_qname_fcn, fname_to_timepoint_fcn=fname_to_timepoint_fcn)

    file_checkers = build_filecheck_generators(opts.imgdatadir, opts.mod_buffer_time,
                                               max_files=opts.max_files, filename_predicate=fname_to_qname_fcn)
    runloop(file_checkers, feeder, opts.poll_time)

if __name__ == "__main__":
    main()