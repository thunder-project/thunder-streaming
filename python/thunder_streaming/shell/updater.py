from abc import abstractmethod
from threading import Thread
from time import sleep


class Updater(Thread):
    """
    The UpdateHandler listens for updates to analysis parameters from any update source and buffers them in its
     message queue. The streaming context will periodically poll each UpdateHandler and route updates to the
     appropriate Analysis objects (in the streaming process).
    """

    def __init__(self, tssc, pause=0):
        Thread.__init__(self)
        self.stop_flag = False
        # Amount of time to pause between each fetch
        self.pause = pause
        self.pub_client = tssc.get_message_proxy().get_publisher()
        self.setDaemon(True)

    @abstractmethod
    def fetch_update(self):
        """
        Abstract method which fetches updates from an update source.

        :return: A (tag, data) tuple (see get_update comment) that will be passed to an Analysis
        """
        return (None, None)

    def stop(self):
        print "Stopping updater thread..."
        self.stop_flag = True

    def run(self):
        """
        Starts retrieving updates. Will run until stop() is called

        :return:
        """
        while not self.stop_flag:
            tag, data = self.fetch_update()
            self.pub_client.publish(tag, data)
            sleep(self.pause)
