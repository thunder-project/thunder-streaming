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

ROOT_SBT_FILE = "build.sbt"
PROJECT_NAME = "thunder-streaming"
SPARK_PATH = os.environ.get("SPARK_PATH")
THUNDER_STREAMING_PATH = os.environ.get("THUNDER_STREAMING_PATH")

class Analysis(object):
    """
    This class is dynamically modified by ThunderStreamingContext when it's initialized with a JAR file
    """

    def __init__(self, name, param_dict):
        self.name = name
        self.param_dict = param_dict

    @staticmethod
    def make_method(name):
        """
        Creates a method that takes a list of parameters and constructs an Analysis object with the correct name
        and parameters dictionary. These attributes will be used by the ThunderStreamingContext to build the XML file
        """
        @staticmethod
        def create_analysis(**params):
            return Analysis(name, params)
        return create_analysis


class Output(object):
    """
    This class is dynamically modified by ThunderStreamingContext when it's initialized with a JAR file
    """
    def __init__(self, name, param_dict):
        self.name = name
        self.param_dict = param_dict

    @staticmethod
    def make_method(name):
        """
        (See the comment in Analysis.make_method)
        """
        @staticmethod
        def create_output( **params):
            return Output(name, params)
        return create_output

class ThunderStreamingContext(object):
    """
    Serves as the main interface to the Scala backend. The main thunder-streaming JAR is searched for Analysis and
    Output classes, and those classes are inserted into the object dictionaries for Analysis and Output
    (as constructors that can take a variable number of arguments, converted to XML params) so that users can type:
    >>> kmeans = Analysis.KMeans(numClusters=5)
    >>> text_output = Outputs.SeriesFileOutput(directory="kmeans_output", prefix="result", include_keys="true")
    >>> tsc.addAnalysis(kmeans, text_output)
    >>> tsc.start()
    Or, if they want to include multiple analysis outputs
    >>> tsc.addAnalysis(kmeans, text_output, binary_output)
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
                setattr(Analysis, fixed_name[-1], Analysis.make_method('.'.join(fixed_name)))
            elif output_regex.match(name):
                print "Found output: %s" % name
                fixed_name = fix_name(name)
                setattr(Output, fixed_name[-1], Output.make_method('.'.join(fixed_name)))
        return ThunderStreamingContext(jar_name)

    def __init__(self, jar_name):
        self.jar_name = jar_name

        # Attributes used in _reinitialize
        self.child = None
        self.doc = None
        self.state = None
        self.config_file = None

        # Set some default run parameters (some must be specified by the user)
        self.run_parameters = {
            'MASTER': 'local[2]',
            'BATCH_TIME': '10',
            'CONFIG_FILE_PATH': None,
            'CHECKPOINT': None,
        }

        def handler(signum, stack):
            self._handle_int()

        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)

        self._reinitialize()

    def _reinitialize(self):

        # Clean up anything leftover from a previous analysis
        self.state = self.STOPPED
        if self.config_file:
            self.config_file.close()
        self.config_file = None
        self.set_config_file_path(None)

        # Reset necessary fields
        self.doc = ET.ElementTree(ET.Element("analyses"))

        self._update_env()

        # The child process has not been created yet
        self.child = None

    def _update_env(self):
        for (name, value) in self.run_parameters.items():
            if value:
                os.putenv(name, value)

    def _get_info_string(self):
        info = "\n"
        info += "JAR Location: %s\n" % self.jar_name
        info += "Spark Location: %s\n" % SPARK_PATH
        info += "Thunder-Streaming Location: %s\n" % THUNDER_STREAMING_PATH
        info += "Checkpointing Directory: %s\n" % self.run_parameters.get("CHECKPOINT", "")
        info += "Master: %s\n" % self.run_parameters.get("MASTER", "")
        info += "Batch Time: %s\n" % self.run_parameters.get("BATCH_TIME", "")
        info += "Configuration Path: %s\n" % self.run_parameters.get("CONFIG_FILE_PATH", "")
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

    def add_analysis(self, analysis, *outputs):

        if self.state == self.STARTED:
            print "You cannot add analyses after the service has been started. Call ThunderStreamingContext.stop() first"
            return

        def build_params(parent, param_dict):
            for (name, value) in param_dict.items():
                param_elem = ET.SubElement(parent, "param")
                param_elem.set("name", name)
                param_elem.set("value", value)

        def build_outputs(analysis):
            for output in outputs:
                output_child = ET.SubElement(analysis, "output")
                name_elem = ET.SubElement(output_child, "name")
                name_elem.text = output.name
                build_params(output_child, output.param_dict)

        analysis_elem = ET.SubElement(self.doc.getroot(), "analysis")
        name_elem = ET.SubElement(analysis_elem, "name")
        name_elem.text = analysis.name
        build_params(analysis_elem, analysis.param_dict)
        build_outputs(analysis_elem)

        # Write the configuration to a temporary file and record the name
        temp = NamedTemporaryFile(delete=False)
        self.config_file = temp
        self.doc.write(temp)
        temp.flush()
        self.set_config_file_path(temp.name)

        # Now the job is ready to run
        self.state = self.READY


    def _kill_child(self):
        """
        Send a SIGTERM signal to the child (Scala process)
        """
        print "\nWaiting up to 10s for the job to stop cleanly..."
        self.child.terminate()
        self.child.poll()
        # Give the child up to 10 seconds to terminate, then force kill it
        for i in xrange(10):
            if self.child and not self.child.returncode:
                time.sleep(1)
                self.child.poll()
            else:
                print "Job stopped cleanly."
                return
        print "Job isn't stopping. Force killing..."
        self.child.kill()

    def _start_child(self):
        """
        Launch the Scala process with the XML file and additional analysis parameters as CLI arguments
        """
        full_jar = os.path.join(os.getcwd(), self.jar_name)
        spark_path = os.path.join(SPARK_PATH, "bin", "spark-submit")
        base_args = [spark_path, "--class", "org.project.thunder.streaming.util.launch.Launcher", full_jar]
        self.child = Popen(base_args)

    def _handle_int(self):
        if self.state == self.STARTED:
            self.stop()

    def start(self):
        if self.state == self.STARTED:
            print "Cannot restart a job once it's already been started. Call ThunderStreamingContext.stop() first."
            return
        if self.state != self.READY:
            print "You need to set up the analyses with ThunderStreamingContext.addAnalysis before the job can be started."
            return
        for (name, value) in self.run_parameters.items():
            if value is None:
                print "Environment variable %s has not been defined (using one of the ThunderStreamingContext.set* methods)." \
                      "It must be set before any analyses can be launched." % name
                return
        print "Starting the streaming analyses with run configuration:"
        print self
        self._start_child()
        self.state = self.STARTED
        # Spin until a SIGTERM or a SIGINT is received
        while not (self.state == self.STOPPED):
            pass

    def stop(self):
        if self.state != self.STARTED:
            print "You can only stop a job that's currently running. Call ThunderStreamingContext.start() first."
            return
        self._kill_child()
        self.state = self.STOPPED
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
        # In Python <2.7 you can't use a ZipFile as a context manager (no 'with' keyword)
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
    if not SPARK_PATH:
        print "SPARK_PATH environment variable isn't set. Please point that to your Spark directory and restart."

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


