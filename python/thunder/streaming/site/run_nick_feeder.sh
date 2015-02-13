#!/bin/bash
PATH_SUBDIR=python/
FEEDER_SUBDIR="$PATH_SUBDIR"/thunder/streaming/feeder/bin

IMAGING_INPUT_DIR=/groups/freeman/freemanlab/Streaming/demo_2015_01_16/registered_im
BEHAV_INPUT_DIR=/groups/freeman/freemanlab/Streaming/demo_2015_01_16/registered_bv
SPARK_OUTPUT_DIR=/nobackup/freeman/streaminginput/
# TMP_OUTPUT_DIR must be on the same filesystem as SPARK_OUTPUT_DIR:
TMP_OUTPUT_DIR=/nobackup/freeman/streamingtmp/
THUNDER_STREAMING_DIR=/groups/freeman/home/swisherj/thunder-streaming
MAX_FILES=40

# local testing directories - leave commented out for use on cluster:
# IMAGING_INPUT_DIR=/mnt/data/data/from_nick/demo_2015_01_09_subset/registered_im/
# BEHAV_INPUT_DIR=/mnt/data/data/from_nick/demo_2015_01_09_subset/registered_bv/
# SPARK_OUTPUT_DIR=/mnt/tmpram/sparkinputdir/
# # TMP_OUTPUT_DIR must be on the same filesystem as SPARK_OUTPUT_DIR:
# TMP_OUTPUT_DIR=/mnt/tmpram/
# THUNDER_STREAMING_DIR=/mnt/data/src/thunder_streaming_mainline_1501
# MAX_FILES=10

# export TMP=$TMP_OUTPUT_DIR
rm "$SPARK_OUTPUT_DIR"/*

# umask 000

PYTHONPATH="$THUNDER_STREAMING_DIR"/"$PATH_SUBDIR" TMP="$TMP_OUTPUT_DIR" \
"$THUNDER_STREAMING_DIR"/"$FEEDER_SUBDIR"/grouping_series_stream_feeder.py \
"$IMAGING_INPUT_DIR"  "$BEHAV_INPUT_DIR"  "$SPARK_OUTPUT_DIR" \
--max-files "$MAX_FILES"  --imgprefix images --behavprefix behaviour -l 60.0
