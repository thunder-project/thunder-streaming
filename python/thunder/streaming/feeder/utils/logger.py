"""Module for logging functionality.

Defines a global logger as `global_logger`.
"""
import logging


class StreamFeederLogger(object):
    """Wrapper around a shared Logger instance.
    """
    def __init__(self, name):
        self._name = name
        self._logger = None
        self._warn_set = None

    def get(self):
        if not self._logger:
            self._logger = logging.getLogger(self._name)
        return self._logger

    def warnIfNotAlreadyGiven(self, *args):
        if self._warn_set is None:
            self._warn_set = set()
        keys = tuple(args)
        if keys not in self._warn_set:
            self._warn_set.add(keys)
            self.get().warn(*args)

global_logger = StreamFeederLogger("streamfeeder")