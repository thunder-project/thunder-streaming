#!/usr/bin/env python
"""
Provides a generator similar to os.walk, with the following differences:
* fully-qualified filenames are returned instead of tuples
* entries within a directory are traversed in lexicographic order
* files created during the iteration will be picked up, provided they are lexicographically later than
the last-yielded filename.

If this file is run as a script, it will print out filenames found in a traversal. Usage is:
python updating_walk.py path_to_directory [time_per_update [start_filename]]
"""
import operator
import os

from thunder_streaming.feeder.utils.logger import global_logger


def updating_walk(dirpath, startpath=None, filefilterfunc=None):
    """Generator function that yields filenames located underneath dirpath.

    Unlike os.walk or os.path.walk, this function will detect files that are created after the initial
    call to the generator.

    Directories will be walked depth-first.

    Parameters
    ----------
    dirpath: string
        Path to an existing directory to serve as the root of the traversal.

    startpath: string, optional, default None
        If passed, specifies a path underneath dirpath from which to begin the traversal. Paths
        lexicographically earlier than startpath will be ignored.

    filefilterfunc: function, optional, default None
        If passed, only files for which `if filefilterfunc(absolute_path_to_file)` evaluates as
        True will be returned. (Files lexicographically earlier than the last returned file will
        still be ignored, regardless of whether they match filefilterfunc).

    Yields
    ------
    string absolute path to next file in traversal.
    """
    # TODO: does not currently correctly handle nesting more that 1 level deep
    isdir, isfile, join, listdir = os.path.isdir, os.path.isfile, os.path.join, os.listdir
    abspath, basename, commonprefix = os.path.abspath, os.path.basename, os.path.commonprefix
    dirname, normpath, relpath = os.path.dirname, os.path.normpath, os.path.relpath

    if not isdir(dirpath):
        raise ValueError("updating_walk must be given a path to an existing directory, got '%s'" % dirpath)

    def get_entries(dirpath_, test, curentry=None, inclusive=False):
        cmpr = operator.ge if inclusive else operator.gt
        if curentry:
            entrynames = [d for d in listdir(dirpath_) if (test(join(dirpath_, d)) and cmpr(d, curentry))]
        else:
            entrynames = [d for d in listdir(dirpath_) if test(join(dirpath_, d))]
        # TODO: sort this in reverse order, so that can do O(1) pop() off end
        entrynames.sort()
        return entrynames

    # find starting point in traversal
    curdir = None
    lastfile = None
    if startpath:
        common = commonprefix([abspath(dirpath), abspath(startpath)])
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
        candidatefile = fnames.pop(0)
        # test against passed filter out here rather than in get_entries
        #   so that we can log files that get filtered out.
        # (get_entries filters based on isfile and isdir, which are not exceptional
        #   conditions when satisfied.)
        if filefilterfunc is not None:
            if not filefilterfunc(candidatefile):
                global_logger.warnIfNotAlreadyGiven("Skipping file: '%s'", join(dirpath, candidatefile))
                continue
        lastfile = candidatefile
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
    nxt = now + t_up
    for fname in updating_walk(sys.argv[1], startpath=start_filename):
        print fname
        now = time.time()
        time.sleep(nxt - now)
        nxt += t_up
