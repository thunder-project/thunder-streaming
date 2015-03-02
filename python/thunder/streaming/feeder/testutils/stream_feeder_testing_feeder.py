#!/usr/bin/env python
"""A testing utility script that produces randomly-generated data files.

Intended use is to simulate a data generation process dropping files in a known directory
at some fixed frequency.
"""
import logging
import os
import random
import string
import time
import sys
import numpy as np

from thunder.streaming.feeder.utils.logger import StreamFeederLogger as Logger


_logger = Logger("feeder-feeder")


def parse_options():
    import optparse
    parser = optparse.OptionParser(usage="%prog datadir [options]")
    parser.add_option("-t", "--time", type="float", default=0.5,
                      help="Time between generating new files in s, default %default")
    parser.add_option("-r", "--runtime", type="float", default=20.0,
                      help="Total program runtime in s, default %default")
    parser.add_option("--datatype", type="choice", choices=("str", "uint16"), default="uint16",
                      help="Type of random data to emit ('str' or 'uint16'), default '%default'")
    parser.add_option("--datalen", type="int", default=512*512*4,
                      help="Length of random file in elements (bytes or uint16), default %default")
    parser.add_option("--datamax", type="int", default=4096,
                      help="Max value (exclusive) of random data, for int types only, default %default")
    parser.add_option("--datafileprefix", type="str", default="img",
                      help="Prepend a prefix to the name of the random files ('' for no prefix), default '%default")
    opts, args = parser.parse_args()

    setattr(opts, "datadir", args[0])

    return opts


def random_string(l):
    return ''.join(random.choice(string.ascii_lowercase) for _ in xrange(l))


def random_array(l, mx=4096, dtype='uint16'):
    return np.random.randint(0, mx, l).astype(dtype)


def write_sequential_file(filenum, datadir, data, ext="txt", prefix="", files_per_subdir=10):
    nsubdir = filenum / files_per_subdir  # integer division
    subdirname = os.path.join(datadir, "%04d" % nsubdir)
    if not os.path.isdir(subdirname):
        os.mkdir(subdirname)
    filepath = os.path.join(subdirname, "%s%06d.%s" % (prefix, filenum, ext))
    with open(filepath, 'w') as fp:
        fp.write(data)
    _logger.get().info("Wrote %s", filepath)


def main():
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(logging.Formatter('%(levelname)s:%(name)s:%(asctime)s:%(message)s'))
    _logger.get().addHandler(_handler)
    _logger.get().setLevel(logging.INFO)

    opts = parse_options()

    if not os.path.isdir(opts.datadir):
        os.mkdir(opts.datadir)

    ext = "txt" if opts.datatype == "str" else "bin"

    filenum = 0
    now = time.time()
    next_time = now + opts.time
    end_time = now + opts.runtime
    while now < end_time:
        time.sleep(next_time - now)

        if opts.datatype == "str":
            buf = random_string(opts.datalen)
        else:
            buf = random_array(opts.datalen, mx=opts.datamax)
        write_sequential_file(filenum, opts.datadir, buf, ext=ext, prefix=opts.datafileprefix)

        next_time += opts.time
        filenum += 1
        now = time.time()

if __name__ == "__main__":
    main()