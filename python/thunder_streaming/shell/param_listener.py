from abc import abstractmethod


class ParamListener(object):
    """
    Abstract base class for anything that handles parameter update notifications from managed MappedScalaClass objects.
    """

    @abstractmethod
    def handle_param_change(self, updated_obj):
        """
        Updates the implementing object to reflect new parameter changes in updated_obj

        :param updated_obj: The object whose parameters have been updated
        :return:
        """
        pass