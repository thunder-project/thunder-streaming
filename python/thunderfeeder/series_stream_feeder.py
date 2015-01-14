#!/usr/bin/env python
"""
"""
import logging
import sys

from stream_feeder import runloop, _logger
from grouping_series_stream_feeder import SyncSeriesFeeder


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
    parser.add_option("--shape", type="int", default=None, nargs=3)
    parser.add_option("--linear", action="store_true", default=False)
    parser.add_option("--dtype", default="uint16")
    parser.add_option("--indtype", default="uint16")
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
    _logger.get().addHandler(_handler)
    _logger.get().setLevel(logging.INFO)

    opts = parse_options()

    feeder = SyncSeriesFeeder(opts.outdir, opts.linger_time, (opts.imgprefix,),
                              shape=opts.shape, dtype=opts.dtype, linear=opts.linear, indtype=opts.indtype)

    runloop((opts.imgdatadir,), feeder, opts.poll_time, opts.mod_buffer_time)

if __name__ == "__main__":
    main()