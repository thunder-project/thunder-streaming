"""Helper functions for parsing passed filenames.
"""
import os


def getFilenamePrefixAndPostfix(filename, delim='_'):
    bname = os.path.splitext(os.path.basename(filename))[0]
    splits = bname.split(delim, 1)
    prefix = splits[0]
    postfix = splits[1] if len(splits) > 1 else ''
    return prefix, postfix


def getFilenamePostfix(filename, delim='_'):
    return getFilenamePrefixAndPostfix(filename, delim)[1]


def getFilenamePrefix(filename, delim='_'):
    return getFilenamePrefixAndPostfix(filename, delim)[0]