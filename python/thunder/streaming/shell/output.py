from python.thunder.streaming.shell.mapped_scala_class import MappedScalaClass

class Output(MappedScalaClass):
    """
    This class is dynamically modified by ThunderStreamingContext when it's initialized with a JAR file
    """

    def __repr__(self):
        desc_str = "Output: \n"
        desc_str += "  Identifier: %s\n" % self.identifier
        desc_str += "  Class: %s\n" % self.full_name
        desc_str += "  Parameters: \n"
        if self._param_dict:
            for (key, value) in self._param_dict.items():
                desc_str += "    %s: %s\n" % (key, value)
        return desc_str

    def __str__(self):
        return self.__repr__()