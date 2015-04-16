#!/usr/bin/python
"""
The main entry point for the Python shell
"""

import optparse as opt
import zipfile
import sys
from subprocess import call

from thunder.streaming.shell.feeder_configuration import *
from thunder.streaming.shell.thunder_streaming_context import *

from thunder.streaming.shell.settings import *

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
                           "to find a suitable JAR file in the current directory hierarchy.", action="store",
                      type="string")
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
tssc = configure_context()
# Import the Analysis and Output objects so that they're available from the shell
from thunder.streaming.shell.analysis import Analysis
from thunder.streaming.shell.output import Output
print "\nAccess the global ThunderStreamingContext through the 'tssc' object"
