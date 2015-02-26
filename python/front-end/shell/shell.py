#!/usr/bin/python

import optparse as opt
import xml.etree.ElementTree as ET
import zipfile
import os
import sys
import re
import signal
import time
from subprocess import Popen, call
from tempfile import NamedTemporaryFile
from abc import abstractmethod
from feeder_configuration import *


ROOT_SBT_FILE = "build.sbt"
PROJECT_NAME = "thunder-streaming"
SPARK_HOME = os.environ.get("SPARK_HOME")
THUNDER_STREAMING_PATH = os.environ.get("THUNDER_STREAMING_PATH")


class MappedScalaClass(object):
    """
    The super class for both Analysis and Output, this class implements generic behaviors like:
     1) Notifying a container class when an object attribute is updated (so the XML file can be regenerated)
     2) Maintaining a counter dictionary so that unique identifiers can be generated (to make it easier to remove
        analyses or outputs after they've been added)
    """

    counter_dict = {}

    @classmethod
    def handle_new_type(cls, new_type):
        cls.counter_dict[new_type] = 0

    @classmethod
    def handle_new_instance(cls, type_name):
        if type_name in cls.counter_dict:
            cur_count = cls.counter_dict[type_name]
            new_count = cur_count + 1
            cls.counter_dict[type_name] = new_count
            # Return a unique identifier for this instance
            return type_name + str(new_count)
        return None

    @classmethod
    def make_method(cls, short_name,  full_name):
        """
        Creates a method that takes a list of parameters and constructs an Analysis object with the correct name
        and parameters dictionary. These attributes will be used by the ThunderStreamingContext to build the XML file
        """
        @staticmethod
        def create_analysis(**params):
            identifier = cls.handle_new_instance(short_name)
            return cls(identifier, full_name, params)
        cls.handle_new_type(short_name)
        return create_analysis

    @staticmethod
    def get_identifier(cls, cls_type):
        if cls_type in cls.counter_dict:
            return cls_type + str(cls.counter_dict[cls_type])
        raise Exception("%s of type %s does not exist." % (cls, cls_type))

    def __init__(self, identifier, full_name, param_dict):
        self._param_dict = param_dict
        self.full_name = full_name
        self.identifier = identifier
        # A handler is notified whenever one of its owned objects is modified
        self._handler = None

    def set_handler(self, handler):
        self._handler = handler

    def update_parameter(self, name, value):
        self._param_dict[name] = value
        self.notify_handler()

    def notify_handler(self):
        if self._handler:
            self._handler.handle_update(self)

    def get_parameters(self):
        # Return a copy so that the actual parameter dictionary is never directly modified
        return self._param_dict.copy()


class UpdateHandler(object):
    """
    Abstract base class for anything that handles parameter update notifications from managed MappedScalaClass objects.
    """

    @abstractmethod
    def handle_update(self, updated_obj):
        pass


class Analysis(MappedScalaClass, UpdateHandler):
    """
    This class is dynamically modified by ThunderStreamingContext when it's initialized with a JAR file
    """

    def __init__(self, identifier, full_name, param_dict):
        super(Analysis, self).__init__(identifier, full_name, param_dict)
        self.outputs = {}

    def add_output(self, *outputs):
        for output in outputs:
            # Output ID must uniquely identify the output in a human-readable way
            self.outputs[output.identifier] = output
            output.set_handler(self)
            self.notify_handler()

    def remove_output(self, maybe_id):
        output_id = None
        if isinstance(maybe_id, str):
            output_id = maybe_id
        elif isinstance(maybe_id, Output):
            output_id = maybe_id.identifier
        if output_id in self.outputs.keys():
            del self.outputs[output_id]
            self.notify_handler()

    def get_outputs(self):
        return self.outputs.values()

    def handle_update(self, updated_obj):
        """
        This method is invoked whenever a child Output is updated. It will propagate the change notification back up to
         the ThunderStreamingContext, which will update the XML file accordingly.
        """
        self.notify_handler()

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

class Output(MappedScalaClass):
    """
    This class is dynamically modified by ThunderStreamingContext when it's initialized with a JAR file
    """

    def __repr__(self):
        desc_str = "Output: \n"
        desc_str += "  Identifier: %s\n" % self.identifier
        desc_str += "  Class: %s\n" % self.full_name
        desc_str += "  Parameters: \n"
        if self._param_dict:
            for (key, value) in self._param_dict.items():
                desc_str += "    %s: %s\n" % (key, value)
        return desc_str

    def __str__(self):
        return self.__repr__()


class ThunderStreamingContext(UpdateHandler):
    """
    Serves as the main interface to the Scala backend. The main thunder-streaming JAR is searched for Analysis and
    Output classes, and those classes are inserted into the object dictionaries for Analysis and Output
    (as constructors that can take a variable number of arguments, converted to XML params) so that users can type:
    >>> kmeans = Analysis.KMeans(numClusters=5)
    >>> text_output = Outputs.SeriesFileOutput(directory="kmeans_output", prefix="result", include_keys="true")
    >>> kmeans.add_output(text_output)
    >>> tsc.add_analysis(kmeans)
    >>> tsc.start()
    Or, if they want to include multiple analysis outputs
    >>> kmeans.add_output(output1, output2, ...)
    >>> tsc.add_analysis(kmeans)
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
            elif output_regex.match(name):
                fixed_name = fix_name(name)
                setattr(Output, fixed_name[-1], Output.make_method(fixed_name[-1], '.'.join(fixed_name)))
        return ThunderStreamingContext(jar_name)

    def __init__(self, jar_name):
        self.jar_name = jar_name

        self.streamer_child = None
        self.doc = None
        self.state = None
        self.config_file = None

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
            # TODO FOR TESTING ONLY
            'PARALLELISM': '100',
            'EXECUTOR_MEMORY': '100G',
            'CONFIG_FILE_PATH': None,
            'CHECKPOINT': None,
        }

        # Gracefully handle SIGINT and SIGTERM signals
        def handler(signum, stack):
            self._handle_int()
        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)
        # self.sig_handler_lock = Lock()

        # Document that will contain the XML specification
        self.doc = ET.ElementTree(ET.Element("analyses"))

        self._reinitialize()

    def _reinitialize(self):

        self._update_env()

        # The child process has not been created yet
        self.streamer_child = None

    def _update_env(self):
        for (name, value) in self.run_parameters.items():
            if value:
                os.putenv(name, value)

    def _get_info_string(self):
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

    def set_master(self, master):
        self.run_parameters['MASTER'] = master
        self._update_env()

    def set_batch_time(self, batch_time):
        self.run_parameters['BATCH_TIME'] = batch_time
        self._update_env()

    def set_config_file_path(self, cf_path):
        self.run_parameters['CONFIG_FILE_PATH'] = cf_path
        self._update_env()

    def set_checkpoint_dir(self, cp_dir):
        self.run_parameters['CHECKPOINT'] = cp_dir
        self._update_env()

    def set_feeder_conf(self, feeder_conf):
        self.feeder_conf = feeder_conf

    def add_analysis(self, analysis):

        if self.state == self.STARTED:
            print "You cannot add analyses after the service has been started. Call ThunderStreamingContext.stop() first"
            return

        def build_params(parent, param_dict):
            for (name, value) in param_dict.items():
                param_elem = ET.SubElement(parent, "param")
                param_elem.set("name", name)
                param_elem.set("value", value)

        def build_outputs(analysis_elem):
            outputs = analysis.get_outputs()
            for output in outputs:
                output_child = ET.SubElement(analysis_elem, "output")
                name_elem = ET.SubElement(output_child, "name")
                name_elem.text = output.full_name
                build_params(output_child, output.get_parameters())

        analysis_elem = ET.SubElement(self.doc.getroot(), "analysis")
        name_elem = ET.SubElement(analysis_elem, "name")
        name_elem.text = analysis.full_name
        build_params(analysis_elem, analysis.get_parameters())
        build_outputs(analysis_elem)

        # Set this TSC as the UpdateHandler for this Analysis instance
        analysis.set_handler(self)

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
        self.add_analysis(updated_obj)

    def _kill_children(self):
        self._kill_child(self.streamer_child, "Streaming server")
        self._kill_child(self.feeder_child, "Feeder process")

    def _start_children(self):

        if self.feeder_conf:
            print "Starting the feeder script with configuration:"
            print self.feeder_conf
            self._start_feeder_child()

        print "Starting the streaming analyses with run configuration:"
        print self
        self._start_streaming_child()

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
        self.feeder_child = Popen(cmd)

    def _start_streaming_child(self):
        """
        Launch the Scala process with the XML file and additional analysis parameters as CLI arguments
        """
        full_jar = os.path.join(os.getcwd(), self.jar_name)
        spark_path = os.path.join(SPARK_HOME, "bin", "spark-submit")
        base_args = [spark_path, "--class", "org.project.thunder.streaming.util.launch.Launcher", full_jar]
        self.streamer_child = Popen(base_args)

    def _handle_int(self):
        # self.sig_handler_lock.acquire()
        if self.state == self.STARTED:
            self.stop()
        # self.sig_handler_lock.release()

    def start(self):
        if self.state == self.STARTED:
            print "Cannot restart a job once it's already been started. Call ThunderStreamingContext.stop() first."
            return
        if self.state != self.READY:
            print "You need to set up the analyses with ThunderStreamingContext.add_analysis before the job can be started."
            return
        if not self.feeder_child:
            print "You have not configured a feeder script to run. Data will be read from the data path specified below (though " \
                "this might not be what you want)."
        for (name, value) in self.run_parameters.items():
            if value is None:
                print "Environment variable %s has not been defined (using one of the ThunderStreamingContext.set* methods)." \
                      "It must be set before any analyses can be launched." % name
                return

        self._start_children()

        self.state = self.STARTED
        # Spin until a SIGTERM or a SIGINT is received
        while self.state == self.STARTED:
            pass

    def stop(self):
        if self.state != self.STARTED:
            print "You can only stop a job that's currently running. Call ThunderStreamingContext.start() first."
            return
        self._kill_children()
        # If execution reaches this point, then an analysis which was previously started has been stopped. Since it can
        # be restarted immediately, the new state is READY
        self.state = self.READY
        self._reinitialize()

    def __repr__(self):
        return self._get_info_string()

    def __str__(self):
        return self._get_info_string()

def in_thunder_streaming():
    """
    :return: True if in the thunder-streaming directory, False otherwise
    """
    project_re = re.compile("^name.*%s" % PROJECT_NAME)
    try:
        with open(ROOT_SBT_FILE, 'r') as f:
            for line in f.readlines():
                if project_re.match(line):
                    return True
            return False
    except IOError as e:
        # File does not exist
        return False

def find_jar(thunder_path=None):
    """
    :return: A ZipFile object wrapping the thunder-streaming JAR, if one could be found, None otherwise
    """
    # TODO: This regex might be too broad
    jar_regex = re.compile("^thunder-streaming.*\.jar")
    ts_package_name = re.compile("^org\/project\/thunder\/streaming")
    matching_jar_names = []
    for root, dir, files in os.walk(thunder_path if thunder_path else "."):
        for file in files:
            if jar_regex.match(file):
                matching_jar_names.append(os.path.join(root, file))
    for jar in matching_jar_names:
        # In Python <2.7 you can't use a ZipFile as a context handler (no 'with' keyword)
        try:
            jar_file = zipfile.ZipFile(jar, 'r')
            # Ensure that this JAR file looks right
            for name in jar_file.namelist():
                if ts_package_name.match(name):
                    return jar, jar_file
            return ThunderStreamingContext()
        except zipfile.BadZipfile:
            continue
    return None, None

def build_project():
    # TODO: This will only work from the project directory
    call(["sbt", "package"])
    return True

def configure_context():
    """
    1) Ensure that the script is being run from the Thunder-Streaming directory (unless a JAR location has been
        explicitly specified, or the $THUNDER_STREAMING_PATH environment variable is set)
    2) Attempt to build the JAR if it doesn't already exist
    3) Examine the JAR to determine which analyses/outputs are available (what's the best way to do this?)
    4) Present the user with a "ThunderContext" object with attributes for each Analysis/Output. The
        ThunderContext is responsible for:
        a) Generating an XML file based on what the user's selected
        b) Providing many of the same Spark configuration methods that the PySpark SparkContext provides, only in this
            case the parameters will be used as CLI arguments to spark-submit
        b) Provide a start() method which invokes spark-submit with the generated XML file and config parameters, then redirects
            filtered output from the child process to this script's stdout
        c) Provide a stop() method which kills the child process
    """

    parser = opt.OptionParser()
    parser.add_option("-j", "--jar", dest="jarfile",
                      help="Optional Thunder-Streaming JAR file location. If this is not specified, this script attempts "
                           "to find a suitable JAR file in the current directory hierarchy.", action="store", type="string")
    parser.add_option("-p", "--py-file", dest="pyfile",
                      help="Optional Python script that will be executed once the ThunderStreamingContext has been initialized.",
                      action="store", type="string")
    (options, args) = parser.parse_args()

    jar_opt = options.jarfile
    py_file = options.pyfile
    if not SPARK_HOME:
        print "SPARK_HOME environment variable isn't set. Please point that to your Spark directory and restart."

    jar_file = None
    if THUNDER_STREAMING_PATH:
        jar_opt, jar_file = find_jar(THUNDER_STREAMING_PATH)
    elif not jar_opt:
        if in_thunder_streaming():
            jar_opt, jar_file = find_jar()
        elif THUNDER_STREAMING_PATH:
            jar_opt, jar_file = find_jar(THUNDER_STREAMING_PATH)
        if not jar_file:
            # The JAR wasn't found, so try to build thunder-streaming, then try to find a JAR file again
            if build_project():
                jar_opt, jar_file = find_jar()
            else:
                print "Could not build the thunder-streaming project. Check your project for errors then try to run this " \
                        "script again."
                sys.exit()
    else:
        jar_file = zipfile.ZipFile(jar_opt, 'r')

    if not jar_file:
        # Couldn't find the JAR. Either we're in the thunder-streaming directory and the project couldn't be built
        # successfully, or we're not in thunder-streaming and --jar wasn't specified
        print "JAR could not be found. Make sure you're either invoking this script in the root thunder-streaming directory," \
              " or with the --jar option."
        sys.exit()

    # Parse the JAR to find relevant classes and construct a ThunderStreamingContext
    return ThunderStreamingContext.fromJARFile(jar_opt, jar_file)

# The following should be executed if the script is imported as a module and also if it's launched standalone
tsc = configure_context()
print "\nAccess the global ThunderStreamingContext through the 'tsc' object"

print NicksFeederConf.generate_command()
