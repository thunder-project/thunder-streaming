from abc import abstractmethod

class Output(object):
    """
    The abstract base class for those classes that take the output from an Analysis/Converter combination and sends
    it to some external location (most likely a visualization server or storage)
    """

    @abstractmethod
    def send(self, converted_output):
        """
        Takes the converted, in-memory representation of an Analysis output, and sends it to an external location.

        :param converted_output: The output from an Analysis that has been converted into a suitable in-memory
        representation (analysis/output specific).
        :return:
        """
        pass