from thunder_streaming.shell.mapped_scala_class import MappedScalaClass
from thunder_streaming.shell.param_listener import ParamListener
import settings
from threading import Thread
import time
import os


class Analysis(MappedScalaClass, ParamListener):
    """
    This class is dynamically modified by ThunderStreamingContext when it's initialized with a JAR file

    An Analysis must have a corresponding thread that monitors its output directory and sends "new data" notifications
    to any converters which have been registered on it.
    """

    SUBSCRIPTION_PARAM = "dr_subscription"
    FORWARDER_ADDR_PARAM = "dr_forwarder_addr"

    # Necessary Analysis parameters
    OUTPUT = "output"

    class FileMonitor(Thread):
        """
        Monitors an Analysis' output directory and periodically sends the entire output from a single timepoint to all
        converters registered on that Analysis
        """

        # Period with which the FileMonitor will check for new data directories
        DIR_POLL_PERIOD = 1
        # Period with which the FileMonitor will check monitored directories for new files
        FILE_POLL_PERIOD = 5
        # Time between each poll
        WAIT_PERIOD = 0.5

        def __init__(self, analysis):
            Thread.__init__(self)
            self.output_dir = analysis.get_output_dir()
            self.outputs = analysis.get_outputs()
            self._stopped = False

            # Fields involved in directory monitoring
            self._last_dir_state = None
            cur_time = time.time()
            self.last_dir_poll = cur_time

            # A dict of 'dir_name' -> (last_state, last_mod_time)
            self.monitored_dirs = {}

        def stop(self):
            self._stopped = True

        def _start_monitoring(self, diff):
            for dir in diff:
                self.monitored_dirs[dir] = (diff, time.time())

        def _qualified_dir_set(self, root):
            if not root:
                return None
            return set([os.path.join(root, f) for f in os.listdir(root)])

        def _qualified_file_set(self, root):
            def path_and_size(f):
                path = os.path.join(root, f)
                size = os.path.getsize(path)
                return (path, size)
            if not root:
                return None
            return set([path_and_size(f) for f in os.listdir(root)])

        def run(self):
            while not self._stopped:
                cur_time = time.time()
                dp = cur_time - self.last_dir_poll
                if dp > self.DIR_POLL_PERIOD:
                    cur_dir_state = self._qualified_dir_set(self.output_dir)
                    if cur_dir_state != self._last_dir_state:
                        if self._last_dir_state != None:
                            diff = cur_dir_state.difference(self._last_dir_state)
                            self._start_monitoring(diff)
                        self._last_dir_state = cur_dir_state
                    self.last_dir_poll = cur_time
                for dir, info in self.monitored_dirs.items():
                    dir_state = self._qualified_file_set(dir)
                    if info[0] != dir_state:
                        self.monitored_dirs[dir] = (dir_state, time.time())
                    elif info[0]:
                        # Only want to get to this point if the directory is not empty
                        if (time.time() - info[1]) > self.FILE_POLL_PERIOD:
                            # The directory has remained the same for a sufficient period of time
                            only_names = map(lambda x: x[0], dir_state)
                            for output in self.outputs:
                                output.handle_new_data(dir, only_names)
                            del self.monitored_dirs[dir]
                time.sleep(self.WAIT_PERIOD)


    def __init__(self, identifier, full_name, param_dict):
        super(Analysis, self).__init__(identifier, full_name, param_dict)
        if Analysis.OUTPUT not in self._param_dict:
            print "An Analysis has been created without an output location. The results of this Analysis will\
             not be usable"
        # self.output is the location which is monitored for new data
        self.output_dir = self._param_dict[Analysis.OUTPUT]
        # The output dictionary is updated in methods that are dynamically inserted into Analysis.__dict__ using the
        # @converter decorator
        self.outputs = []

        self.file_monitor = Analysis.FileMonitor(self)

        # Put the address of the subscription forwarder into the parameters dict
        self._param_dict[Analysis.FORWARDER_ADDR_PARAM] = "tcp://" + settings.MASTER  + ":" + str(settings.PUB_PORT)
        self.receive_updates(self)

    def get_output_dir(self):
        return self.output_dir

    def get_outputs(self):
        return self.outputs

    def add_output(self, output):
        self.outputs[output.identifier] = output

    def receive_updates(self, analysis):
        """
        Write a parameter to the Analysis' XML file that tells it to subscribe to updates from the given
        analysis.
        """
        existing_subs = self._param_dict.get(Analysis.SUBSCRIPTION_PARAM)
        identifier = analysis if isinstance(analysis, str) else analysis.identifier
        if not existing_subs:
            new_sub = [identifier]
            self.update_parameter(Analysis.SUBSCRIPTION_PARAM, new_sub)
        else:
            existing_subs.append(identifier)
            self.update_parameter(Analysis.SUBSCRIPTION_PARAM, existing_subs)

    def start(self):
        self.file_monitor.start()

    def stop(self):
        self.file_monitor.stop()

    def __repr__(self):
        desc_str = "Analysis: \n"
        desc_str += "  Identifier: %s\n" % self.identifier
        desc_str += "  Class: %s\n" % self.full_name
        desc_str += "  Parameters: \n"
        if self._param_dict:
            for (key, value) in self._param_dict.items():
                desc_str += "    %s: %s\n" % (key, value)
        return desc_str

    def __str__(self):
        return self.__repr__()
