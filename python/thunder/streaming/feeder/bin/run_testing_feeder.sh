#!/bin/bash
BASEDIR=/mnt/tmpram
BEHAV_DELAY=5.0
CWD=`pwd`

$CWD/stream_feeder_testing_feeder.py $BASEDIR/feederimgs --datafileprefix img_ &
sleep $BEHAV_DELAY
$CWD/stream_feeder_testing_feeder.py $BASEDIR/feederbehav --datalen 2 --datamax 128 --datafileprefix behav_ &
