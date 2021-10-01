#!/bin/bash
#
# Entrypoint for an app that runs together with PostgreSQL.
#

# TODO(Grisha): remove this file and use the amp entrypoint #106.

set -e

umask 000

#source ~/.bash_profile

export PYTHONPATH=/app:$PYTHONPATH
echo "PYTHONPATH=$PYTHONPATH"

# Activate virtual environment to access Python packages.
source /${ENV_NAME}/bin/activate

# Initialize the DB.
# ./im/devops/init_im_db.py --db $POSTGRES_DB

eval "$@"
