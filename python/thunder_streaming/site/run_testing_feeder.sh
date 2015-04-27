#!/bin/bash
PATH_SUBDIR=python/
FEEDER_SUBDIR="$PATH_SUBDIR"/thunder/streaming/feeder/bin

THUNDER_STREAMING_DIR=/mnt/data/src/thunder_streaming_mainline_1501
BASEDIR=/mnt/tmpram
BEHAV_DELAY=5.0

PYTHONPATH="$THUNDER_STREAMING_DIR"/"$PATH_SUBDIR" \
"$FEEDER_SUBDIR"/testutils/stream_feeder_testing_feeder.py "$BASEDIR"/feederimgs \
--datafileprefix img_ &
sleep "$BEHAV_DELAY"
"$FEEDER_SUBDIR"/testutils/stream_feeder_testing_feeder.py "$BASEDIR"/feederbehav \
--datalen 2 --datamax 128 --datafileprefix behav_ &
