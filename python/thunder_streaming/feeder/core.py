"""Core functions used by the Thunder streaming feeder scripts, including asynchronous checking for new files.
"""
import errno
import os
import time

from thunder.streaming.feeder.utils.filenames import getFilenamePostfix, getFilenamePrefix
from thunder.streaming.feeder.utils.logger import global_logger
from thunder.streaming.feeder.utils.regex import RegexMatchToQueueName, RegexMatchToTimepointString
from thunder.streaming.feeder.utils.updating_walk import updating_walk as uw


def file_check_generator(source_dir, mod_buffer_time, max_files=-1, filename_predicate=None):
    """Generator function that polls the passed directory tree for new files, using the updating_walk.py logic.

    This generator will restart the underlying updating_walk at the last seen file if the updating walk runs
    out of available files.
    """
    next_batch_file, walker_restart_file = None, None
    walker = uw(source_dir, filefilterfunc=filename_predicate)
    while True:
        filebatch = []
        files_left = max_files
        try:
            if not next_batch_file:
                next_batch_file = next(walker)
                walker_restart_file = next_batch_file

            delta = time.time() - os.stat(next_batch_file).st_mtime
            while delta > mod_buffer_time and files_left:
                filebatch.append(next_batch_file)
                files_left -= 1
                next_batch_file = None  # reset in case of exception on next line
                next_batch_file = next(walker)
                delta = time.time() - os.stat(next_batch_file).st_mtime
                walker_restart_file = next_batch_file

        except StopIteration:
            # no files left, restart after polling interval
            if not filebatch:
                global_logger.get().info("Out of files, waiting...")
            walker = uw(source_dir, walker_restart_file, filefilterfunc=filename_predicate)
        yield filebatch


def build_filecheck_generators(source_dir_or_dirs, mod_buffer_time, max_files=-1, filename_predicate=None):
    if isinstance(source_dir_or_dirs, basestring):
        source_dirs = [source_dir_or_dirs]
    else:
        source_dirs = source_dir_or_dirs

    file_checkers = [file_check_generator(source_dir, mod_buffer_time,
                                          max_files=max_files, filename_predicate=filename_predicate)
                     for source_dir in source_dirs]
    return file_checkers


def runloop(file_checkers, feeder, poll_time):
    """ Main program loop. This will check for new files in the passed input directories using file_check_generator,
    push any new files found into the passed Feeder subclass via its feed() method, wait for poll_time,
    and repeat forever.
    """
    last_time = time.time()
    while True:
        for file_checker in file_checkers:
            # this should never throw StopIteration, will just yield an empty list if nothing is avail:
            filebatch = feeder.feed(next(file_checker))
            if filebatch:
                global_logger.get().info("Pushed %d files, last: %s", len(filebatch), os.path.basename(filebatch[-1]))

        removedfiles = feeder.clean()
        if removedfiles:
            global_logger.get().info("Removed %d temp files, last: %s", len(removedfiles), os.path.basename(removedfiles[-1]))

        next_time = last_time + poll_time
        try:
            time.sleep(next_time - time.time())
        except IOError, e:
            if e.errno == errno.EINVAL:
                # passed a negative number, which is fine, just don't sleep
                pass
            else:
                raise e
        last_time = next_time


def get_parsing_functions(opts):
    if opts.prefix_regex_file:
        fname_to_qname_fcn = RegexMatchToQueueName.fromFile(opts.prefix_regex_file).queueName
    else:
        fname_to_qname_fcn = getFilenamePrefix
    if opts.timepoint_regex_file:
        fname_to_timepoint_fcn = RegexMatchToTimepointString.fromFile(opts.timepoint_regex_file).timepoint
    else:
        fname_to_timepoint_fcn = getFilenamePostfix
    return fname_to_qname_fcn, fname_to_timepoint_fcn