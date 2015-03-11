from thunder.streaming.shell.mapped_scala_class import MappedScalaClass
from thunder.streaming.shell.param_listener import ParamListener
import settings


class Analysis(MappedScalaClass, ParamListener):
    """
    This class is dynamically modified by ThunderStreamingContext when it's initialized with a JAR file
    """

    SUBSCRIPTION_PARAM = "dr_subscription"
    FORWARDER_ADDR_PARAM = "dr_forwarder_addr"

    def __init__(self, identifier, full_name, param_dict):
        super(Analysis, self).__init__(identifier, full_name, param_dict)
        self.outputs = {}
        # Put the address of the subscription forwarder into the parameters dict
        self._param_dict[Analysis.FORWARDER_ADDR_PARAM] = "tcp://" + settings.MASTER  + ":" + str(settings.SUB_PORT)

    def add_output(self, *outputs):
        for output in outputs:
            # Output ID must uniquely identify the output in a human-readable way
            self.outputs[output.identifier] = output
            output.set_handler(self)
        self.notify_param_listener()

    def receive_updates(self, analysis):
        """
        Write a parameter to the Analysis' XML file that tells it to subscribe to updates from the given
        analysis.
        """
        existing_subs = self._param_dict.get(Analysis.SUBSCRIPTION_PARAM)
        if not existing_subs:
            existing_subs = [analysis]
        identifier = analysis if isinstance(analysis, str) else analysis.identifier
        if not existing_subs:
            new_sub = [identifier]
            self.update_parameter(Analysis.SUBSCRIPTION_PARAM, new_sub)
        else:
            existing_subs.append(identifier)
            self.update_parameter(Analysis.SUBSCRIPTION_PARAM, existing_subs)

    def remove_output(self, maybe_id):
        output_id = None
        if isinstance(maybe_id, str):
            output_id = maybe_id
        elif isinstance(maybe_id, Output):
            output_id = maybe_id.identifier
        if output_id in self.outputs.keys():
            del self.outputs[output_id]
            self.notify_param_listener()

    def get_outputs(self):
        return self.outputs.values()

    def handle_param_change(self, updated_obj):
        """
        This method is invoked whenever a child Output is updated. It will propagate the change notification back up to
         the ThunderStreamingContext, which will update the XML file accordingly.
        """
        self.notify_param_listener()

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