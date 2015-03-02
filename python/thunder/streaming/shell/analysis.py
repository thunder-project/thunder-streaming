from python.thunder.streaming.shell.mapped_scala_class import MappedScalaClass
from python.thunder.streaming.shell.shell import UpdateHandler
from python.thunder.streaming.shell.output import Output

__author__ = 'osheroffa'


class Analysis(MappedScalaClass, UpdateHandler):
    """
    This class is dynamically modified by ThunderStreamingContext when it's initialized with a JAR file
    """

    def __init__(self, identifier, full_name, param_dict):
        super(Analysis, self).__init__(identifier, full_name, param_dict)
        self.outputs = {}

    def add_output(self, *outputs):
        for output in outputs:
            # Output ID must uniquely identify the output in a human-readable way
            self.outputs[output.identifier] = output
            output.set_handler(self)
            self.notify_handler()

    def remove_output(self, maybe_id):
        output_id = None
        if isinstance(maybe_id, str):
            output_id = maybe_id
        elif isinstance(maybe_id, Output):
            output_id = maybe_id.identifier
        if output_id in self.outputs.keys():
            del self.outputs[output_id]
            self.notify_handler()

    def get_outputs(self):
        return self.outputs.values()

    def handle_update(self, updated_obj):
        """
        This method is invoked whenever a child Output is updated. It will propagate the change notification back up to
         the ThunderStreamingContext, which will update the XML file accordingly.
        """
        self.notify_handler()

    def __repr__(self):
        desc_str = "Analysis: \n"
        desc_str += "  Identifier: %s\n" % self.identifier
        desc_str += "  Class: %s\n" % self.full_name
        desc_str += "  Parameters: \n"
        if self._param_dict:
            for (key, value) in self._param_dict.items():
                desc_str += "    %s: %s\n" % (key, value)
        if self.outputs:
            desc_str += "  Outputs: \n"
            for output in self.outputs.values():
                desc_str += "    %s: %s\n" % (output.identifier, output.full_name)
        return desc_str

    def __str__(self):
        return self.__repr__()