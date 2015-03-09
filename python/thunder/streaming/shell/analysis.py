from thunder.streaming.shell.mapped_scala_class import MappedScalaClass
from thunder.streaming.shell.param_listener import ParamListener
from thunder.streaming.shell.update_handler import Updatable
from abc import abstractmethod
from socket import create_connection
from Queue import Queue, Empty
from threading import Thread
from ctypes import c_int64
from struct import pack

class DataSender(Thread, Updatable):
    """
    TODO RENAME CLASS OBVIOUSLY
    The DataSender communicates with an Analysis' corresponding Scala object via some communication channel (chosen
    by subclasses). The channel chosen might vary depending on what resources are available.

    A threadsafe queue is necessary for queueing messages in order to prevent multiple UpdateHandler threads from
    attempting to write to the socket at once
    """

    def __init__(self):
        self.msg_queue = Queue()
        self.stop = False

    def handle_update(self, data):
        self.msg_queue.put(data)

    def stop(self):
        self.stop = True

    def start(self):
        while not self.stop:
            new_data = None
            try:
                new_data = self.msg_queue.get_nowait()
            except Empty:
                pass
            self.send(new_data)

    @abstractmethod
    def send(self, data):
        pass

    @staticmethod
    def get_data_sender():
        """
        DataSender factory
        """
        return TCPSender


class TCPSender(DataSender):
    """
    Sends raw update data to a Scala analysis through a TCP channel
    """

    class Header(object):
        """
        A 64-bit header containing the message size
        """

        def __init__(self, data):
            # Data should be a string at this point
            if isinstance(data, str):
                self.size = len(data)
            else:
                # The data object is invalid
                self.size = -1

        def get_size(self):
            return self.size

    def __init__(self, **kwargs):
        DataSender.__init__(self)
        self.host = kwargs.get('host')
        self.port = kwargs.get('port')
        self.socket = None
        self.reconnect()

    def reconnect(self):
        if self.host and self.port:
            self.socket = create_connection((self.host, self.port))
        if not self.socket:
            print "Could not open a socket connection to %s:%s" % (self.host, self.port)

    def send(self, data):
        if not self.socket:
            self.reconnect()
        if self.socket:
            header = TCPSender.Header(data)
            size = header.get_size()
            packed_data = pack("q%ss" % size, size, data)
            self.socket.sendall(packed_data)


class Analysis(MappedScalaClass, ParamListener):
    """
    This class is dynamically modified by ThunderStreamingContext when it's initialized with a JAR file
    """

    def __init__(self, identifier, full_name, param_dict):
        super(Analysis, self).__init__(identifier, full_name, param_dict)
        self.outputs = {}
        self.data_sender = None

    def add_output(self, *outputs):
        for output in outputs:
            # Output ID must uniquely identify the output in a human-readable way
            self.outputs[output.identifier] = output
            output.set_handler(self)
        self.notify_param_listener()

    def add_updater(self, updater):
        updater.add_listener(self.identifier, self.data_sender)

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

    def start(self):
        """
        Launch the DataSender, which should try to connect to the Scala Analysis
        :return:
        """
        if self.data_sender:
            self.data_sender.start()
        else:
            print "DataSender not specified for %s. This Analysis will not be dynamically updated." % self.identifier

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