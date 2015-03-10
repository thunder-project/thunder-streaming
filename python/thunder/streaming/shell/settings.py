"""
Global settings for the shell module
"""
import os

ROOT_SBT_FILE = "build.sbt"
PROJECT_NAME = "thunder-streaming"
SPARK_HOME = os.environ.get("SPARK_HOME")
THUNDER_STREAMING_PATH = os.environ.get("THUNDER_STREAMING_PATH")
SUB_PORT = 8060
PUB_PORT = 8061