from thunder.streaming.shell.updater import Updater
import random


class ExampleUpdater(Updater):

    def __init__(self, tssc, tag):
        Updater.__init__(self, tssc, pause=10)
        self.tag = tag

    def fetch_update(self):
        num = random.randrange(0, 100)
        print "Sending update: (%s, %s)" % (self.tag, str(num))
        return self.tag, str(num)
