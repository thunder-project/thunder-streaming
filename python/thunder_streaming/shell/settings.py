"""
Global settings for the shell module
"""
import os

ROOT_SBT_FILE = "build.sbt"
PROJECT_NAME = "thunder-streaming"
SPARK_HOME = os.environ.get("SPARK_HOME")
THUNDER_STREAMING_PATH = os.environ.get("THUNDER_STREAMING_PATH")
# TODO: This also needs to be determined dynamically
SUB_PORT = 9060
PUB_PORT = 9061
# TODO: How do we determine this more generally?
MASTER = "localhost"