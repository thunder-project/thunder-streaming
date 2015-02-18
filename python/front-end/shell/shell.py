
import optparse as opt
import xml.dom.minidom as xml
import zipfile
import os
import re
from subprocess import Popen, call

ROOT_SBT_FILE = "build.sbt"
PROJECT_NAME = "thunder-streaming"

class Analysis(object):
    """
    This class is dynamically modified by ThunderStreamingContext when it's initialized with a JAR file
    """

    def __init__(self, name, param_dict):
        self.name = name
        self.param_dict = param_dict

    @staticmethod
    def makeMethod(name):
        """
        Creates a method that takes a list of parameters and constructs an Analysis object with the correct name
        and parameters dictionary. These attributes will be used by the ThunderStreamingContext to build the XML file
        """
        def createAnalysis(**params):
            return Analysis(name, params)
        createAnalysis


class Output(object):
    """
    This class is dynamically modified by ThunderStreamingContext when it's initialized with a JAR file
    """
    def __init__(self, name, param_dict):
        self.name = name
        self.param_dict = param_dict

    @staticmethod
    def makeMethod(name):
        """
        (See the comment in Analysis.makeMethod)
        """
        def createOutput(**params):
            return Output(name, params)
        createOutput

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

    STARTED = "running"
    STOPPED = "stopped"
    INITED = "inited"

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
                print "Found analysis: %s" % name
                fixed_name = fix_name(name)
                setattr(Analysis, fixed_name[-1], Analysis.makeMethod('.'.join(fixed_name)))
            elif output_regex.match(name):
                print "Found output: %s" % name
                fixed_name = fix_name(name)
                setattr(Output, fixed_name[-1], Output.makeMethod('.'.join(fixed_name)))
        ThunderStreamingContext(jar_name)

    def __init__(self):
        impl = xml.getDOMImplementation()
        self.doc = impl.createDocument(None, "analyses", None)
        self.state = ThunderStreamingContext.INITED

    def addAnalysis(self, analysis, *outputs):

        if self.state == ThunderStreamingContext.STARTED:
            print "You cannot add analyses after the service has been started. Call ThunderStreamingContext.stop() first"
            return

        def build_params(param_dict):
            params = []
            for (name, value) in param_dict.items():
                param_elem = self.doc.createElement("param")
                param_elem.setAttribute("name", name)
                param_elem.setAttribute("value", value)
                params.append(param_elem)

        def build_outputs():
            outputs = []
            for output in outputs:
                output_child = self.doc.createElement("output")
                output_params = build_params(output.param_dict)
                for param_elem in output_params:
                    output_child.appendChild(param_elem)
                outputs.append(output)

        analysis_elem = self.doc.createElement("analysis")
        analysis_params = build_params(analysis.param_dict)
        outputs = build_outputs()
        for param_elem in analysis_params:
            analysis_elem.appendChild(param_elem)
        for output_elem in outputs:
            analysis_elem.appendChild(output_elem)

        self.doc.appendChild(analysis_elem)

    def _kill_child(self):
        """
        Self a SIGTERM signal to the child (Scala process)
        """

    def start(self):
        # Start the child process with the XML file, and additional analysis parameters, as CLI arguments
        self._start_child()
        self.state = ThunderStreamingContext.STARTED

    def stop(self):
        # Kill the child process
        self._kill_child()
        self.state = ThunderStreamingContext.STOPPED

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

if __name__ == '__main__':
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
    (options, args) = parser.parse_args()

    jar_opt = options.jarfile
    thunder_path = os.environ.get("THUNDER_STREAMING_PATH")

    jar_file = None
    if not jar_opt and in_thunder_streaming():
        jar_opt, jar_file = find_jar()
        if not jar_file:
            # The JAR wasn't found, so try to build thunder-streaming, then try to find a JAR file again
            if build_project():
                jar_opt, jar_file = find_jar()
            else:
                print "Could not build the thunder-streaming project. Check your project for errors then try to run this " \
                        "script again."
                exit(-1)
    elif not jar_opt and thunder_path:
        jar_opt, jar_file = find_jar(thunder_path)

    if not jar_file:
        # Couldn't find the JAR. Either we're in the thunder-streaming directory and the project couldn't be built
        # successfully, or we're not in thunder-streaming and --jar wasn't specified
        print "JAR could not be found. Make sure you're either invoking this script in the root thunder-streaming directory," \
              " or with the --jar option."
        exit(-1)

    # Parse the JAR to find relevant classes and construct a ThunderStreamingContext
    tsc = ThunderStreamingContext.fromJARFile(jar_opt,jar_file)

