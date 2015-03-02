__author__ = 'osheroffa'


class MappedScalaClass(object):
    """
    The super class for both Analysis and Output, this class implements generic behaviors like:
     1) Notifying a container class when an object attribute is updated (so the XML file can be regenerated)
     2) Maintaining a counter dictionary so that unique identifiers can be generated (to make it easier to remove
        analyses or outputs after they've been added)
    """

    counter_dict = {}

    @classmethod
    def handle_new_type(cls, new_type):
        cls.counter_dict[new_type] = 0

    @classmethod
    def handle_new_instance(cls, type_name):
        if type_name in cls.counter_dict:
            cur_count = cls.counter_dict[type_name]
            new_count = cur_count + 1
            cls.counter_dict[type_name] = new_count
            # Return a unique identifier for this instance
            return type_name + str(new_count)
        return None

    @classmethod
    def make_method(cls, short_name,  full_name):
        """
        Creates a method that takes a list of parameters and constructs an Analysis object with the correct name
        and parameters dictionary. These attributes will be used by the ThunderStreamingContext to build the XML file
        """
        def create_analysis(**params):
            identifier = cls.handle_new_instance(short_name)
            return cls(identifier, full_name, params)
        cls.handle_new_type(short_name)
        return create_analysis

    @staticmethod
    def get_identifier(cls, cls_type):
        if cls_type in cls.counter_dict:
            return cls_type + str(cls.counter_dict[cls_type])
        raise Exception("%s of type %s does not exist." % (cls, cls_type))

    def __init__(self, identifier, full_name, param_dict):
        self._param_dict = param_dict
        self.full_name = full_name
        self.identifier = identifier
        # A handler is notified whenever one of its owned objects is modified
        self._handler = None

    def set_handler(self, handler):
        self._handler = handler

    def update_parameter(self, name, value):
        self._param_dict[name] = value
        self.notify_handler()

    def notify_handler(self):
        if self._handler:
            self._handler.handle_update(self)

    def get_parameters(self):
        # Return a copy so that the actual parameter dictionary is never directly modified
        return self._param_dict.copy()