#!/usr/bin/env bash
#
# Configure bash profile.
#

# TODO(gp): -> init_bash_profile.sh

set -e

echo "Configuring bash"

BASH_INIT=~/.bash_profile
touch $BASH_INIT
echo "source /${ENV_NAME}/bin/activate" >>$BASH_INIT
echo "set -o vi" >>$BASH_INIT
