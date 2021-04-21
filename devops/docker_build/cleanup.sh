#!/usr/bin/env bash
#
# Clean up.
#

set -e
source ~/.bash_profile

DIRS="/usr/lib/gcc /app/tmp.pypoetry /root/.cache /tmp"

du -hs $DIRS | sort -h

rm -rf $DIRS
