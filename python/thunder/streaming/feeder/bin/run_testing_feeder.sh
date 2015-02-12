#!/bin/bash
FEEDER_SUBDIR=python/thunder/streaming/feeder/

BASEDIR=/mnt/tmpram
BEHAV_DELAY=5.0

"$FEEDER_SUBDIR"/testutils/stream_feeder_testing_feeder.py "$BASEDIR"/feederimgs \
--datafileprefix img_ &
sleep "$BEHAV_DELAY"
"$FEEDER_SUBDIR"/testutils/stream_feeder_testing_feeder.py "$BASEDIR"/feederbehav \
--datalen 2 --datamax 128 --datafileprefix behav_ &
