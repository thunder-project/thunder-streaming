from thunder.streaming.shell.mapped_scala_class import MappedScalaClass
from thunder.streaming.shell.param_listener import ParamListener
from thunder.streaming.shell.update_handler import Updatable
from abc import abstractmethod
from Queue import Queue, Empty
from threading import Thread
from ctypes import c_int64
from struct import pack
import socket
from time import sleep

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
        self.initialize()
        while not self.stop:
            new_data = None
            try:
                new_data = self.msg_queue.get_nowait()
            except Empty:
                pass
            self.send(new_data)

    @abstractmethod
    def initialize(self):
        """
        Initialization that should be performed from inside the DataSender thread
        """
        pass

    @abstractmethod
    def send(self, data):
        pass

    @abstractmethod
    def get_description(self):
        """
        Returns a description of this DataSender which is sent to the Scala Analysis so that it can establish a
        connection.
        """
        pass

    @staticmethod
    def get_data_sender(**kwargs):
        """
        DataSender factory
        """
        return TCPSender(kwargs)



class TCPSender(DataSender):
    """
    Sends raw update data to a Scala analysis through a TCP channel

    TODO: Better error handling in the event that a connection is never established
    """

    NUM_CONN_RETRIES = 10
    NUM_SEND_RETRIES = 5
    RETRY_WAIT_TIME = 3

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

    class Ack(object):
        """
        A 1-byte response returned from the Analysis once it's received an update
        """
        SUCCESS = 1
        FAILURE = 0

        @staticmethod
        def is_success(byte):
            if int(byte) & TCPSender.Ack.SUCCESS:
                return True
            else:
                return False

    def __init__(self, **kwargs):
        DataSender.__init__(self)
        self.host = kwargs.get('host', 'localhost')
        self.port = kwargs.get('port', 0)
        self.recv_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.send_socket = None
        self.listening = False

    def initialize(self):
        self._start_server()

    def _start_server(self):
        for i in xrange(TCPSender.NUM_CONN_RETRIES):
            try:
                self.recv_socket = self.recv_socket.bind((self.host, self.port))
                self.port = self.recv_socket.getsockname()[1]
                self.recv_socket.listen(5)
                self.listening = True
                break
            except Exception as e:
                print "Error in TCPSender._start_server: %s" % e
                sleep(TCPSender.RETRY_WAIT_TIME)
        if not self.listening:
            print "Could not bind a socket connection to %s:%s" % (self.host, self.port)

    def _wait_for_conn(self):
        if not self.listening:
            self._start_server()
        if self.listening:
            (conn, addr) = self.recv_socket.accept()
            self.send_socket = conn

    def send(self, data):
        if not self.send_socket:
            self._wait_for_conn()
        if not self.send_socket:
            print "DataServer could not be started. Cannot update the analysis."
            return
        header = TCPSender.Header(data)
        size = header.get_size()
        if size != -1:
            packed_data = pack("q%ss" % size, size, data)
        else:
            print "In TCPSender.send, data is not properly formed. Cannot update the analysis."
        for i in xrange(TCPSender.NUM_SEND_RETRIES):
            self.send_socket.sendall(packed_data)
            rsp = self.send_socket.recv(1)
            if TCPSender.Ack.is_success(rsp):
                return
            else:
                print "Sending message to analysis failed. Retry attempt %d..." % i
        print "Failed to send message to analysis after %d retries." % TCPSender.NUM_SEND_RETRIES

    def get_description(self):
        return {
            'host': self.host,
            'port': self.port
        }


class Analysis(MappedScalaClass, ParamListener):
    """
    This class is dynamically modified by ThunderStreamingContext when it's initialized with a JAR file
    """

    def __init__(self, identifier, full_name, param_dict):
        super(Analysis, self).__init__(identifier, full_name, param_dict)
        self.outputs = {}
        # Create an instance of the default DataSender
        self.data_sender = DataSender.get_data_sender()
        self.data_sender.start()
        # Now that the DataSender has been started, get its description and add it to the param dictionary
        desc = self.data_sender.get_description()
        for k,v in desc.items():
            # DataSender parameters are prefixed with a 'ds_' so that there aren't namespace collisions between
            # Analysis params and DataSender params
            self.update_parameter('ds_'+k, v)

    def add_output(self, *outputs):
        for output in outputs:
            # Output ID must uniquely identify the output in a human-readable way
            self.outputs[output.identifier] = output
            output.set_handler(self)
        self.notify_param_listener()

    def receive_updates(self, updater, tag=None):
        """
        Bind the Updater to this Analysis' DataSender. Whenever an update is received from an external source, it
        will be inserted into the DataSender's queue.
        """
        if not tag:
            tag = self.identifier
        updater.add_listener(tag, self.data_sender)

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