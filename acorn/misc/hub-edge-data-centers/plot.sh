#!/bin/bash

set -e
set -u

SRC_DIR=`dirname $BASH_SOURCE`

export FN_OUT=$SRC_DIR/hub-edge-data-centers.pdf
gnuplot $SRC_DIR/hub-edge-data-center.gnuplot 2>&1 | sed 's/^/  /'
if [ "${PIPESTATUS[0]}" -ne "0" ]; then
	exit 1
fi
printf "Created %s %d\n" $FN_OUT `wc -c < $FN_OUT`
