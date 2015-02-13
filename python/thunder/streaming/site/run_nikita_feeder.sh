#!/bin/bash
PATH_SUBDIR=python/
FEEDER_SUBDIR="$PATH_SUBDIR"/thunder/streaming/feeder/bin

IMAGING_INPUT_DIR=/groups/ahrens/ahrenslab/Nikita/Realtime/imaging/test1_*
EPHYS_INPUT_DIR=/groups/ahrens/ahrenslab/Nikita/Realtime/ephys/
SPARK_OUTPUT_DIR=/nobackup/freeman/streaminginput/
# TMP_OUTPUT_DIR must be on the same filesystem as SPARK_OUTPUT_DIR:
TMP_OUTPUT_DIR=/nobackup/freeman/streamingtmp/
THUNDER_STREAMING_DIR=/groups/freeman/home/swisherj/thunder-streaming
MOD_TIME=5
MAX_FILES=-1  # disable rate limiting

# local testing directories - leave commented out for use on cluster:
# IMAGING_INPUT_DIR=/mnt/data/data/nikita_mock/imgin*/
# EPHYS_INPUT_DIR=/mnt/data/data/nikita_mock/behavinput/
# SPARK_OUTPUT_DIR=/mnt/tmpram/sparkinputdir/
# # TMP_OUTPUT_DIR must be on the same filesystem as SPARK_OUTPUT_DIR:
# TMP_OUTPUT_DIR=/mnt/tmpram/
# THUNDER_STREAMING_DIR=/mnt/data/src/thunder_streaming_mainline_1501
# MOD_TIME=5
# MAX_FILES=2  # turn on rate limiting for simulated runs

# export TMP=$TMP_OUTPUT_DIR
rm "$SPARK_OUTPUT_DIR"/*

PYTHONPATH="$THUNDER_STREAMING_DIR"/"$PATH_SUBDIR" TMP="$TMP_OUTPUT_DIR" \
"$THUNDER_STREAMING_DIR"/"$FEEDER_SUBDIR"/grouping_series_stream_feeder.py \
"$IMAGING_INPUT_DIR"  "$EPHYS_INPUT_DIR"  "$SPARK_OUTPUT_DIR" \
--prefix-regex-file "$THUNDER_STREAMING_DIR"/resources/regexes/nikita_queuenames.regex \
--timepoint-regex-file "$THUNDER_STREAMING_DIR"/resources/regexes/nikita_timepoints.regex \
--max-files "$MAX_FILES" --check-size -m "$MOD_TIME"
