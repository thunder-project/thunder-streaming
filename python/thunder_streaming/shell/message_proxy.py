
import zmq
from zmq.devices import ThreadDevice
from threading import Thread
import settings


class Publisher(object):
    """
    Publishers wrap ZeroMQ PUB sockets, which publish messages to the MessageProxy. These messages are then
    forwarded to all subscribers (generally Analysis objects in the streamer process).
    """

    def __init__(self, context, addr):
        """
        Given the host/port of the XSUB proxy, create a PUB socket connected to that proxy
        """
        self.pub_sock = context.socket(zmq.PUB)
        self.pub_sock.connect(addr)

    def publish(self, tag, msg):
        """
        Publish a message to all self.tag subscribers
        """
        try:
            self.pub_sock.send_multipart([str(tag), str(msg)])
        except Exception as e:
            print e

    @staticmethod
    def get_publisher(context, addr):
        return Publisher(context, addr)


class Subscriber(object):
    """
    Subscribers wrap ZeroMQ SUB sockets, which subscribe to messages from the MessageProxy.
    """

    POLL_TIME = 10

    def __init__(self, context, addr, tag):
        """
        Given the host/port of the XSUB proxy, create a PUB socket connected to that proxy
        """
        self.sub_sock = context.socket(zmq.SUB)
        self.sub_sock.connect(addr)
        self.tag = tag
        # If the tag was specified when the object was constructed, subscribe now
        if self.tag:
            self.pub_sock.setsockopt(zmq.SUBSCRIBE, self.tag)

    def subscribe(self, tag):
        self.pub_sock.setsockopt(zmq.SUBSCRIBE, tag)

    def _receive(self):
        [address, msg] = self.sub_sock.recv_multipart()
        return msg

    def receive(self, blocking=True):
        """
        Do a (blocking or non-blocking) recv operation on the underlying socket
        """
        if blocking or self.sub_sock.poll(Subscriber.POLL_TIME):
            return self._receive()
        return None

    @staticmethod
    def get_subscriber(context, addr, tag):
        return Subscriber(context, addr, tag)


class MessageProxy(object):
    """
    The MessageProxy wraps a ZeroMQ proxy server (XSUB socket) which forwards 'publish' messages from Python Analysis
    objects to subscribers (Scala Analysis objects) in the streamer process.
    """

    DEFAULT_NUM_THREADS = 1

    # In-process communication settings
    INPROC_PUB_ID = "msg_proxy_pub"
    INPROC_SUB_ID = "msg_proxy_sub"

    # Remote communication settings
    PUB_PORT = settings.PUB_PORT
    SUB_PORT = settings.SUB_PORT

    # TODO: This will only work if the Python front-end and the streamer process are running on the same machine
    DEFAULT_BIND_HOST = "localhost"

    def __init__(self, host=DEFAULT_BIND_HOST, num_threads=DEFAULT_NUM_THREADS):
        self.initialized = False
        self.context = zmq.Context(num_threads)
        self.host = host

        self.device_thread = None

    def start(self):
        # Launch two proxy sockets, one for remote communication, and one for in-process communication.
        self.device_thread = ThreadDevice(zmq.FORWARDER, zmq.XSUB, zmq.XPUB)
        self.device_thread.bind_in("tcp://*:" + str(MessageProxy.SUB_PORT))
        self.device_thread.bind_out("tcp://*:" + str(MessageProxy.PUB_PORT))
        self.device_thread.start()

        # TODO Finish in-process communication
        """
        self.local_frontend = self.context.socket(zmq.XSUB)
        self.local_frontend.bind("inproc://" + MessageProxy.INPROC_SUB_ID)
        self.local_backend = self.context.socket(zmq.XPUB)
        self.local_backend.bind("inproc://" + MessageProxy.INPROC_PUB_ID)
        zmq.device(zmq.FORWARDER, self.local_frontend, self.local_backend)
        """

        self.initialized = True

    def _get_pub_addr(self, remote=True):
        if remote:
            return "tcp://" + self.host + ":" + str(MessageProxy.SUB_PORT)
        else:
            return "inproc://" + MessageProxy.INPROC_SUB_ID

    def _get_sub_addr(self, remote=True):
        if remote:
            return "tcp://" + self.host + ":" + str(MessageProxy.PUB_PORT)
        else:
            return "inproc://" + MessageProxy.INPROC_PUB_ID

    def get_publisher(self, remote=True):
        """
        :param tag: The tag the client will use to publish
        :param remote: True if the client wishes to publish to remote services, False otherwise.
        :return: A Publisher object constructed using this MessageProxy's ZMQ Context
        """
        return Publisher.get_publisher(self.context, self._get_pub_addr(remote))

    def get_subscriber(self, tag=None, remote=True):
        """
        :param tag: The tag the client will use to subscribe
        :param remote: True if the client wishes to subscribe to remote services, False otherwise.
        :return: A Subscriber object constructed using this MessageProxy's ZMQ Context
        """
        return Subscriber.get_subscriber(self.context, self._get_sub_addr(remote), tag)


