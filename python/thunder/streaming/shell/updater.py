from abc import abstractmethod
from threading import Thread
from time import sleep

class Updatable(object):
    """
    Interface for a class that can handle analysis updates from an external source
    """
    @abstractmethod
    def handle_update(self, data):
        pass


class Updater(Thread):
    """
    The UpdateHandler listens for updates to analysis parameters from any update source and buffers them in its
     message queue. The streaming context will periodically poll each UpdateHandler and route updates to the
     appropriate Analysis objects (in the streaming process).
    """

    def __init__(self, pause=0):
        # A map from listener ID to listener
        self.listeners = {}
        self.stop = False
        # Amount of time to pause between each fetch
        self.pause = pause

    def add_listener(self, lid, listener):
        if lid not in self.listeners:
            self.listeners[lid] = []
        self.listeners[lid].append(listener)

    @abstractmethod
    def fetch_update(self):
        """
        Abstract method which fetches updates from an update source.

        :return: A (tag, data) tuple (see get_update comment) that will be passed to an Analysis
        """
        return (None, None)

    def stop(self):
        self.stop = True

    def start(self):
        """
        Starts retrieving updates. Will run until stop() is called

        :return:
        """
        while not self.stop:
            tag, data = self.fetch_update()
            if tag in self.listeners:
                listeners = self.listeners[tag]
                for listener in listeners:
                    listener.handle_update(data)
            sleep(self.paused)
