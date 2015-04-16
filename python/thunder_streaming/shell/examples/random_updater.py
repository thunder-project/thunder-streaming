from thunder_streaming.shell.updater import Updater
import random


class RandomUpdater(Updater):

    def __init__(self, tssc, tag):
        Updater.__init__(self, tssc, pause=10)
        self.tag = tag

    def fetch_update(self):
        num = random.randrange(0, 100)
        return self.tag, str(num)
