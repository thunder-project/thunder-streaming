"""Functions to convert input binary files (one per time point) into Thunder series formatted output files.
"""

import numpy as np


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
    """Transposes the contents of the passed filenames into a new (large) in-memory buffer
    """
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