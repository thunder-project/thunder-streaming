#!/usr/bin/env python
"""An elaboration on grouping_stream_feeder, which transposes matching file pairs into the Thunder series binary
format instead of just copying them as-is.

Expected usage: something like:
 ./grouping_series_stream_feeder.py \
 /mnt/data/data/from_nick/demo_2015_01_09/registered_im/ \
 /mnt/data/data/from_nick/demo_2015_01_09/registered_bv/  \
 /mnt/tmpram/sparkinputdir/ \
 -l -1.0 --imgprefix images --behavprefix behaviour --shape 512 512 4

 Example using regexes to specify queues and timepoints:
 ./grouping_series_stream_feeder.py \
 /mnt/data/data/nikita_mock/imginput/ \
 /mnt/data/data/nikita_mock/behavinput/ \
 /mnt/tmpram/sparkinputdir/ \
 -l -1.0 --prefix-regex-file ../../resources/regexes/nikita_queuenames.regex \
 --timepoint-regex-file ../../resources/regexes/nikita_timepoints.regex

 Set TMP environment var to same filesystem as output directory (here /mnt/tmpram/)
 so as to ensure that os.rename step is atomic - see stream_feeder.py.

Behavioral vars will be represented as an extra, incomplete 'z' dimension
Regular image data can be extracted in thunder as something like the following:
imgseries = series.filterOnKeys(lambda (x, y, z): z < 4)

If a --shape parameter is passed to the script, the resulting output files will have x,y,z subscript indices
added to match the specified shape. If no shape is passed, then the output will not have any index set (not even
a linear index). (It is not clear (to me) whether data without any index could be read as a Series by Thunder...)

This script expects input binary data to be written with contigous z-planes. So for instance,
if input data is in the expected format, the following should yield a sensible image:

import numpy as np
import pylab as p
rawim = np.fromfile("path/to/flat/binary/file.bin", dtype="data-dtype")
rawim.shape = (zdim, ydim, xdim)
p.imshow(rawim[zplane])
p.show()

Given data in this format, the script should be called with --shape xdim ydim zdim, with dimensions
specified in x, y, z order. (This is consistent with the behavior of the rest of Thunder.)

"""
import glob
import logging
import os
import sys

from thunder_streaming.feeder.utils.logger import global_logger
from thunder_streaming.feeder.core import build_filecheck_generators, runloop, get_parsing_functions
from thunder_streaming.feeder.feeders import SyncSeriesFeeder


def get_last_matching_directory(directory_path_pattern):
    dirnames = [fname for fname in glob.glob(directory_path_pattern) if os.path.isdir(fname)]
    if dirnames:
        return sorted(dirnames)[-1]
    raise ValueError("No directories found matching pattern '%s'" % directory_path_pattern)


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
    parser.add_option("--shape", type="int", default=None, nargs=3)
    parser.add_option("--linear", action="store_true", default=False)
    parser.add_option("--dtype", default="uint16")
    parser.add_option("--indtype", default="uint16")
    parser.add_option("--prefix-regex-file", default=None)
    parser.add_option("--timepoint-regex-file", default=None)
    parser.add_option("--check-size", action="store_true", default=False,
                      help="If set, assume all files should be the same size as the first encountered file of that " +
                           "type, and discard with a warning files that have different sizes.")
    parser.add_option("--no-check-skip",  dest="check_skip", action="store_false", default=True,
                      help="If set, omit checking for skipped timepoints. Default is to warn if " +
                           "a timepoint appears to have been missed.")
    opts, args = parser.parse_args()

    if len(args) != 3:
        print >> sys.stderr, parser.get_usage()
        sys.exit(1)

    setattr(opts, "imgdatadir", get_last_matching_directory(args[0]))
    setattr(opts, "behavdatadir", get_last_matching_directory(args[1]))
    setattr(opts, "outdir", args[2])

    return opts


def main():
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(logging.Formatter('%(levelname)s:%(name)s:%(asctime)s:%(message)s'))
    global_logger.get().addHandler(_handler)
    global_logger.get().setLevel(logging.INFO)

    opts = parse_options()

    global_logger.get().info("Reading images from: %s", opts.imgdatadir)
    global_logger.get().info("Reading behavioral/ephys data from: %s", opts.behavdatadir)

    fname_to_qname_fcn, fname_to_timepoint_fcn = get_parsing_functions(opts)
    feeder = SyncSeriesFeeder(opts.outdir, opts.linger_time, (opts.imgprefix, opts.behavprefix),
                              shape=opts.shape, dtype=opts.dtype, indtype=opts.indtype,
                              fname_to_qname_fcn=fname_to_qname_fcn,
                              fname_to_timepoint_fcn=fname_to_timepoint_fcn,
                              check_file_size=opts.check_size,
                              check_skip_in_sequence=opts.check_skip)
    file_checkers = build_filecheck_generators((opts.imgdatadir, opts.behavdatadir), opts.mod_buffer_time,
                                               max_files=opts.max_files,
                                               filename_predicate=fname_to_qname_fcn)
    runloop(file_checkers, feeder, opts.poll_time)

if __name__ == "__main__":
    main()