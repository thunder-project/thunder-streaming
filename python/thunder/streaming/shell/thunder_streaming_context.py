from thunder.streaming.shell.analysis import Analysis
from thunder.streaming.shell.param_listener import ParamListener
from thunder.streaming.shell.message_proxy import MessageProxy
from thunder.streaming.shell.settings import *
from thunder.streaming.shell.converter import *

import signal
import re
import xml.etree.ElementTree as ET
from tempfile import NamedTemporaryFile
from subprocess import Popen
import time
import atexit


class ThunderStreamingContext(ParamListener):
    """
    Serves as the main interface to the Scala backend. The main thunder-streaming JAR is searched for Analysis and
    Output classes, and those classes are inserted into the object dictionaries for Analysis and Output
    (as constructors that can take a variable number of arguments, converted to XML params) so that users can type:
    >>> kmeans = Analysis.KMeans(numClusters=5)
    >>> text_output = Outputs.SeriesFileOutput(directory="kmeans_output", prefix="result", include_keys="true")
    >>> kmeans.add_output(text_output)
    >>> tssc.add_analysis(kmeans)
    >>> tssc.start()
    Or, if they want to include multiple analysis outputs
    >>> kmeans.add_output(output1, output2, ...)
    >>> tssc.add_analysis(kmeans)
    """

    STARTED = "started"
    STOPPED = "stopped"
    READY = "ready"

    @staticmethod
    def fromJARFile(jar_name, jar_file):
        """
        Takes a ZipFile representing the thunder-streaming JAR as input, iterates through the list of classes and
        creates a mapping from human-readable Analysis/Analysis object name to fully-qualified Scala class name in
        the Analysis/Output class dict
        """
        # TODO: Your classes should be renamed to make more sense.
        analysis_regex = re.compile("^.*\/[^\/]+Analysis\.class")
        output_regex = re.compile("^.*\/[^\/]+Output\.class")

        def fix_name(name):
            # Replace / with . and strip the .class from the end
            stripped_name = name[:-6]
            return stripped_name.split("/")

        for name in jar_file.namelist():
            if analysis_regex.match(name):
                fixed_name = fix_name(name)
                setattr(Analysis, fixed_name[-1], Analysis.make_method(fixed_name[-1], '.'.join(fixed_name)))
        return ThunderStreamingContext(jar_name)

    def __init__(self, jar_name):
        self.jar_name = jar_name

        self.streamer_child = None
        self.doc = None
        self.state = None
        self.config_file = None
        self.analyses = {}

        # The feeder script responsible for copying properly chunked output from one directory (usually the one
        # specified by the user) to the temporary directory being monitored by the Scala process (which is passed
        # to it in the XML file)
        self.feeder_child = None
        # The FeederConfiguration used to start the feeder_child
        self.feeder_conf = None

        # Set some default run parameters (some must be specified by the user)
        self.run_parameters = {
            'MASTER': 'local[2]',
            'BATCH_TIME': '10',
            'CONFIG_FILE_PATH': None,
            'CHECKPOINT': None,
            # TODO FOR TESTING ONLY
            'PARALLELISM': None,
            'EXECUTOR_MEMORY': None,
            'CHECKPOINT_INTERVAL': '10000',
            'HADOOP_BLOCK_SIZE': '1'
        }

        # Build setters for each existing run parameter
        for key in self.run_parameters.keys():
            def setter_builder(key):
                def param_setter(value):
                    self.run_parameters[key] = str(value)
                    self._update_env()
                return param_setter
            self.__dict__["set_"+key.lower()] = setter_builder(key)

        # Gracefully handle SIGINT and SIGTERM signals
        def handler(signum, stack):
            self._handle_int()

        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)

        # ZeroMQ messaging proxy
        self.updaters = []
        self.message_proxy = MessageProxy()
        self.message_proxy.start()
        print "MessageProxy is running..."

        atexit.register(self.destroy)

        self._reset_document()
        self._reinitialize()

    def get_message_proxy(self):
        return self.message_proxy

    def _reset_document(self):
        # Document that will contain the XML specification
        self.doc = ET.ElementTree(ET.Element("analyses"))
        self._write_document()

    def _reinitialize(self):

        self._update_env()

        # The child process has not been created yet
        self.streamer_child = None

    def _update_env(self):
        for (name, value) in self.run_parameters.items():
            if value:
                os.putenv(name, value)

    def set_feeder_conf(self, feeder_conf):
        self.feeder_conf = feeder_conf
        self._update_env()

    def add_updater(self, updater):
        self.updaters.append(updater)

    def add_analysis(self, analysis):

        if self.state == self.STARTED:
            print "You cannot add analyses after the service has been started. Call ThunderStreamingContext.stop() first"
            return

        self.analyses[analysis.identifier] = analysis
        # Set this TSSC as the ParamListener for this Analysis instance
        analysis.set_param_listener(self)
        self._write_document()

    def remove_analysis(self, analysis):
        if analysis.identifier in self.analyses:
            del self.analyses[analysis.identifier]
        self._reset_document()

    def _write_document(self):

        def build_params(parent, param_dict):
            for (name, value) in param_dict.items():
                # Shortest way to do this
                vs = [value] if not isinstance(value, list) else value
                for v in vs:
                    param_elem = ET.SubElement(parent, "param")
                    param_elem.set("name", name)
                    param_elem.set("value", v)


        for analysis in self.analyses.values():

            analysis_elem = ET.SubElement(self.doc.getroot(), "analysis")
            name_elem = ET.SubElement(analysis_elem, "name")
            name_elem.text = analysis.full_name
            build_params(analysis_elem, analysis.get_parameters())

        # Write the configuration to a temporary file and record the name
        temp = NamedTemporaryFile(delete=False)
        self.config_file = temp
        self.doc.write(temp)
        temp.flush()
        self.set_config_file_path(temp.name)

        # Now the job is ready to run
        self.state = self.READY

    def handle_update(self, updated_obj):
        """
        Called when an Analysis has been modified. Updates the XML file to reflect the modifications
        :param updated_obj:  An Analysis whose parameters or Outputs have been modified
        :return: None (updates the XML file)
        """
        if self.state == self.STARTED:
            self.stop()
        if self.config_file:
            self.config_file.close()
            self.set_config_file_path(None)
        self._reset_document()

    def _kill_children(self):
        self._kill_child(self.streamer_child, "Streaming server")
        if self.feeder_child:
            self._kill_child(self.feeder_child, "Feeder process")

    def _start_children(self):

        print "Starting the Analysis threads."
        self._start_analyses()

        print "Starting the updaters."
        self._start_updaters()

        print "Starting the streaming analyses with run configuration:"
        print self
        self._start_streaming_child()

        sleep_time = 5
        print "Sleeping for %d seconds before starting the feeder script..." % sleep_time
        time.sleep(sleep_time)

        if self.feeder_conf:
            print "Starting the feeder script with configuration:"
            print self.feeder_conf
            self._start_feeder_child()

    def _kill_child(self, child, name):
        """
        Send a SIGTERM signal to the child (Scala process)
        """
        print "\nWaiting up to 10s for the %s to stop cleanly..." % name
        child.terminate()
        child.poll()
        # Give the child up to 10 seconds to terminate, then force kill it
        for i in xrange(10):
            if child and not child.returncode:
                time.sleep(1)
                child.poll()
            else:
                print "%s stopped cleanly." % name
                return
        print "%s isn't stopping. Force killing..." % name
        child.kill()

    def _start_feeder_child(self):
        if not self.feeder_conf:
            print "You must set the feeder script configuration (using self.set_feeder_conf) before starting the feeder."
            return
        (env_vars, cmd) = self.feeder_conf.generate_command()
        for (key, value) in env_vars.items():
            os.putenv(key, value)

        # Remove any files remaining in the feeder's output directory (the streamer's input
        # directory), as they can only be leftovers from a previous analysis and are unusable.
        input_dir = self.feeder_conf.params.get('spark_input_dir')
        if input_dir:
            for path in [os.path.abspath(os.path.join(input_dir, f)) for f in os.listdir(input_dir)]:
                os.remove(path)

        self.feeder_child = Popen(cmd)

    def _start_analyses(self):
        for analysis in self.analyses.values():
            analysis.start()

    def _start_updaters(self):
        for updater in self.updaters:
            updater.start()

    def _start_streaming_child(self):
        """
        Launch the Scala process with the XML file and additional analysis parameters as CLI arguments
        """
        full_jar = os.path.join(os.getcwd(), self.jar_name)
        spark_path = os.path.join(SPARK_HOME, "bin", "spark-submit")
        base_args = [spark_path, "--jars",
                     ",".join([os.path.join(THUNDER_STREAMING_PATH, "scala/project/lib/jeromq-0.3.4.jar"),
                     os.path.join(THUNDER_STREAMING_PATH, "scala/project/lib/spray-json_2.10-1.3.1.jar")]),
                     "--class", "org.project.thunder.streaming.util.launch.Launcher", full_jar]
        self.streamer_child = Popen(base_args)

    def destroy(self):
        print "In tssc.destroy..."
        for updater in self.updaters:
            updater.stop()

    def _handle_int(self):
        if self.state == self.STARTED:
            print "Calling self.stop() in ThunderStreamingContext"
            self.stop()

    def start(self):
        if self.state == self.STARTED:
            print "Cannot restart a job once it's already been started. Call ThunderStreamingContext.stop() first."
            return
        if self.state != self.READY:
            print "You need to set up the analyses with ThunderStreamingContext.add_analysis before the job can be started."
            return
        if not self.feeder_conf:
            print "You have not configured a feeder script to run. Data will be read from the data path specified below (though " \
                  "this might not be what you want)."
        for (name, value) in self.run_parameters.items():
            if value is None:
                print "Environment variable %s has not been defined (using one of the ThunderStreamingContext.set* methods)." \
                      "It must be set before any analyses can be launched." % name
                return

        self._start_children()
        self.state = self.STARTED

    def stop(self):
        if self.state != self.STARTED:
            print "You can only stop a job that's currently running. Call ThunderStreamingContext.start() first."
            return
        for analysis in self.analyses.values():
            analysis.stop()
        self._kill_children()
        # If execution reaches this point, then an analysis which was previously started has been stopped. Since it can
        # be restarted immediately, the new state is READY
        self.state = self.READY
        self._reinitialize()

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        info = "ThunderStreamingContext: \n"
        info += "  JAR Location: %s\n" % self.jar_name
        info += "  Spark Location: %s\n" % SPARK_HOME
        info += "  Thunder-Streaming Location: %s\n" % THUNDER_STREAMING_PATH
        info += "  Checkpointing Directory: %s\n" % self.run_parameters.get("CHECKPOINT", "")
        info += "  Master: %s\n" % self.run_parameters.get("MASTER", "")
        info += "  Batch Time: %s\n" % self.run_parameters.get("BATCH_TIME", "")
        info += "  Configuration Path: %s\n" % self.run_parameters.get("CONFIG_FILE_PATH", "")
        info += "  State: %s\n" % self.state
        return info
