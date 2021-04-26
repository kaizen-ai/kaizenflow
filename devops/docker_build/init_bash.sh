#!/usr/bin/env bash
#
# Configure bash.
#

set -e

echo "Configuring bash"

BASH_INIT=~/.bash_profile
touch $BASH_INIT
echo "source /${ENV_NAME}/bin/activate" >>$BASH_INIT
echo "set -o vi" >>$BASH_INIT
