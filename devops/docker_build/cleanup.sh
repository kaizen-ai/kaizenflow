#!/usr/bin/env bash
#
# Clean up.
#

set -e
source ~/.bash_profile

#DIRS="/usr/lib/gcc /app/tmp.pypoetry /root/.cache /tmp"
DIRS="/app/tmp.pypoetry"

du -hs $DIRS | sort -h

rm -rf $DIRS
