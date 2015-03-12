from thunder.streaming.shell.updater import Updater
import json


class FilteringUpdater(Updater):
    """
    Publishes a list of keys
    """

    def __init__(self, tssc, tag):
        Updater.__init__(self, tssc, pause=10)
        self.tag = tag
        self.key_range = [1, 2, 3, 4, 5]

    def fetch_update(self):
        print "Sending update: (%s, %s)" % (self.tag, str(self.key_range))
        return self.tag, json.dumps(self.key_range)