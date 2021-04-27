#!/usr/bin/env bash
#
# Clean up.
#

# TODO(gp): Move this in the proper layer to save image space.

set -e

DIRS="/usr/lib/gcc /app/tmp.pypoetry /root/.cache /tmp"

du -hs $DIRS | sort -h

rm -rf $DIRS
