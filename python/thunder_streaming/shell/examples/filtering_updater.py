from thunder_streaming.shell.updater import Updater
import json


class FilteringUpdater(Updater):
    """
    Publishes a list of keys
    """

    def __init__(self, tssc, tag):
        Updater.__init__(self, tssc, pause=10)
        self.tag = tag
        self.send_count = 0

    def fetch_update(self):
        key_range = [1, 2, 3, 4, 5] if self.send_count > 5 else []
        self.send_count += 1
        return self.tag, json.dumps(key_range)