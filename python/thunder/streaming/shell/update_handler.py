from abc import abstractmethod

__author__ = 'osheroffa'


class UpdateHandler(object):
    """
    Abstract base class for anything that handles parameter update notifications from managed MappedScalaClass objects.
    """

    @abstractmethod
    def handle_update(self, updated_obj):
        pass