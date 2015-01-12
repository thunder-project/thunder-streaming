#!/usr/bin/env python
"""
Provides a generator similar to os.walk, with the following differences:
* fully-qualified filenames are returned instead of tuples
* entries within a directory are traversed in lexicographic order
* new files that are lexicographically later than the last-returned entry will be picked up
"""
import operator
import os


def updating_walk(dirpath, startpath=None):
    # TODO: does not currently correctly handle nesting more that 1 level deep
    isdir, isfile, join, listdir = os.path.isdir, os.path.isfile, os.path.join, os.listdir
    abspath, basename, commonprefix = os.path.abspath, os.path.basename, os.path.commonprefix
    dirname, normpath, relpath = os.path.dirname, os.path.normpath, os.path.relpath

    if not isdir(dirpath):
        raise ValueError("updating_walk must be given a path to an existing directory, got '%s'" % dirpath)

    def get_entries(dirpath_, test, curentry=None, inclusive=False):
        cmp = operator.ge if inclusive else operator.gt
        if curentry:
            entrynames = [d for d in listdir(dirpath_) if (test(join(dirpath_, d)) and cmp(d, curentry))]
        else:
            entrynames = [d for d in listdir(dirpath_) if test(join(dirpath_, d))]
        entrynames.sort()
        return entrynames

    # find starting point in traversal
    curdir = None
    lastfile = None
    if startpath:
        common = commonprefix([abspath(dirpath), abspath(startpath)])
        # if normpath(common) != normpath(dirpath):
        #     raise ValueError("If passed, startpath must be rooted at dirpath. Got dirpath, startpath: (%s, %s)" %
        #                      (dirpath, startpath))
        if normpath(common) == normpath(dirpath):
            startsubpath = relpath(startpath, dirpath)
            if isdir(startpath):
                curdir = startsubpath
                lastfile = None
            else:
                curdir = dirname(startsubpath)
                lastfile = basename(startsubpath)

    # traverse directories first
    dirs = get_entries(dirpath, isdir, curdir, inclusive=True)
    while dirs:
        curdir = dirs.pop(0)
        for entry in updating_walk(join(dirpath, curdir), startpath):
            yield entry
        dirs = get_entries(dirpath, isdir, curdir)

    # traverse files in this directory
    fnames = get_entries(dirpath, isfile, lastfile)
    while fnames:
        lastfile = fnames.pop(0)
        yield join(dirpath, lastfile)
        fnames = get_entries(dirpath, isfile, lastfile)

if __name__ == "__main__":
    import sys
    import time
    if len(sys.argv) not in (2, 3, 4):
        print >> sys.stderr, "Usage: updating_walk.py path_to_directory [time_per_update [start_filename]]"

    t_up = float(sys.argv[2]) if len(sys.argv) > 2 else 1.0
    start_filename = sys.argv[3] if len(sys.argv) > 3 else ""
    now = time.time()
    next = now + t_up
    for fname in updating_walk(sys.argv[1], startpath=start_filename):
        print fname
        now = time.time()
        time.sleep(next - now)
        next += t_up
