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
import logging
import os
import sys

import tempfile

import numpy as np

from stream_feeder import runloop, _logger
from grouping_stream_feeder import SyncCopyAndMoveFeeder, getFilenamePrefix, getFilenamePostfix, getParsingFunctions


def transpose_files(filenames, outfp, dtype='uint16'):
    """Rewrites the flat binary files whose names are given in 'filenames' into a single flat binary
    output file.

    The first element in the output will be the first element of the first passed file. The second element
    will be the first element of the second file, up to the Nth element for N passed filenames. The N+1st
    element in the output file will be the second element of the first passed file, and so on.

    This corresponds to a Thunder binary series file, except without keys.
    """
    outbuf = None
    nfiles = len(filenames)
    ary_size = 0
    for fnidx, fn in enumerate(filenames):
        ary = np.fromfile(fn, dtype=dtype)
        if outbuf is None:
            ary_size = ary.size
            totsize = ary_size * nfiles
            outbuf = np.empty((totsize,), dtype=dtype)
        outbuf[fnidx::nfiles] = ary
    if outbuf is not None:
        outbuf.tofile(outfp)
    return ary_size  # number of distinct indices written


def _write_series_records(filenames, ndim=1, dtype='uint16', indtype='uint16'):
    outbuf = None
    ary_size = 0
    incr = len(filenames) + ndim
    for fnidx, fn in enumerate(filenames):
        ary = np.fromfile(fn, dtype=indtype).astype(dtype)
        if outbuf is None:
            ary_size = ary.size
            totsize = ary_size * incr  # (nelts per image * (n images + ndim))
            outbuf = np.empty((totsize,), dtype=dtype)
        outbuf[(fnidx+ndim)::incr] = ary
    return outbuf, ary_size


def transpose_files_to_series(filenames, outfp, shape, dtype='uint16', indtype='uint16', startlinidx=0):
    """Rewrites the flat binary files whose names are given in 'filenames' into a valid Thunder binary series
    file, including keys.

    'startlinidx' gives the linear index of the first element to be written out in this call. By setting
    startlinidx = prod(shape), this allows subscript indices to be written that are greater than fit into the
    specified shape. This is expected to be useful in appending behavioral regressor data at the end of an
    otherwise valid image series.
    """
    nfiles = len(filenames)
    incr = nfiles + len(shape)
    outbuf, ary_size = _write_series_records(filenames, ndim=len(shape), dtype=dtype, indtype=indtype)

    # check whether we are about to exceed the allowable range for the array size
    while (startlinidx + ary_size) >= np.prod(shape):
        shape = list(shape[:-1]) + [shape[-1] + 1]  # keep adding 1 to last (z) dimension until we're ok

    subidxarys = np.unravel_index(np.arange(startlinidx, startlinidx + ary_size,
                                            dtype=np.uint32), shape, order='F')
    for subidx, subidxary in enumerate(subidxarys):
        outbuf[subidx::incr] = subidxary
    if outbuf is not None:
        outbuf.tofile(outfp)
    return ary_size


def transpose_files_to_linear_series(filenames, outfp, dtype='uint32', indtype='uint16', startlinidx=0):
    """Rewrites the flat binary files whose names are given in 'filenames' into a valid Thunder binary series
    file, including linear keys.
    """
    nfiles = len(filenames)
    incr = nfiles + 1
    outbuf, ary_size = _write_series_records(filenames, ndim=1, dtype=dtype, indtype=indtype)

    ddtype = np.dtype(dtype)
    maxval = np.iinfo(ddtype).max if ddtype.kind in ('i', 'u') else np.finfo(ddtype).max
    if startlinidx + ary_size >= maxval:
        raise ValueError("Type '%s' isn't large enough to represent linear indices; " % str(dtype) +
                         "max index is %d, max representable val is %d" % (startlinidx + ary_size, int(maxval)))

    linidxs = np.arange(startlinidx, startlinidx + ary_size)
    if outbuf is not None:
        outbuf[::incr] = linidxs
        outbuf.tofile(outfp)
    return ary_size


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
                 fname_to_qname_fcn=getFilenamePrefix, fname_to_timepoint_fcn=getFilenamePostfix):
        super(SyncSeriesFeeder, self).__init__(feeder_dir, linger_time, prefixes,
                                               fname_to_qname_fcn=fname_to_qname_fcn,
                                               fname_to_timepoint_fcn=fname_to_timepoint_fcn)
        self.prefixes = list(prefixes)
        self.shape = shape
        self.linear = linear
        self.dtype = dtype
        self.indtype = indtype

    def get_series_filename(self, srcfilenames, bytesize):
        startcount = self.fname_to_timepoint_fcn(srcfilenames[0])
        endcount = self.fname_to_timepoint_fcn(srcfilenames[-1])
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
                newname = self.get_series_filename(filenames, recordsize)

                # touch prior to atomic move operation to delay slurping by spark
                os.utime(tmpfname, None)
                os.rename(tmpfname, os.path.join(self.feeder_dir, newname))
            finally:
                if not tmpfp.closed:
                    tmpfp.close()
                if os.path.isfile(tmpfname):
                    os.remove(tmpfname)
        return fullnames


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
    parser.add_option("--shape", type="int", default=None, nargs=3)
    parser.add_option("--linear", action="store_true", default=False)
    parser.add_option("--dtype", default="uint16")
    parser.add_option("--indtype", default="uint16")
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
    _logger.get().addHandler(_handler)
    _logger.get().setLevel(logging.INFO)

    opts = parse_options()

    fname_to_qname_fcn, fname_to_timepoint_fcn = getParsingFunctions(opts)
    feeder = SyncSeriesFeeder(opts.outdir, opts.linger_time, (opts.imgprefix, opts.behavprefix),
                              shape=opts.shape, dtype=opts.dtype, indtype=opts.indtype,
                              fname_to_qname_fcn=fname_to_qname_fcn,
                              fname_to_timepoint_fcn=fname_to_timepoint_fcn)

    runloop((opts.imgdatadir, opts.behavdatadir), feeder, opts.poll_time, opts.mod_buffer_time)

if __name__ == "__main__":
    main()