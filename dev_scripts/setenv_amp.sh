#
# Configure the local (thin) client built with `dev_scripts/client_setup/build.sh`.
#

# TODO(gp): -> dev_scripts -> dev_scripts_amp

PWD=$(pwd)
AMP=$PWD

# #############################################################################
# Virtual env
# #############################################################################

# This needs to be in sync with dev_scripts/client_setup/build.sh
VENV_DIR="$HOME/src/venv/amp.client_venv"
if [[ ! -d $VENV_DIR ]]; then
    echo "Can't find VENV_DIR='$VENV_DIR': checking the container one"
    # The venv in the container is in a different spot. Check that.
    VENV_DIR="/venv/amp.client_venv"
    if [[ ! -d $VENV_DIR ]]; then
        echo "ERROR: Can't find VENV_DIR='$VENV_DIR'"
        return -1
    fi;
fi;

ACTIVATE_SCRIPT="$VENV_DIR/bin/activate"
echo "# Activate virtual env '$ACTIVATE_SCRIPT'"
if [[ ! -f $ACTIVATE_SCRIPT ]]; then
    echo "ERROR: Can't find '$ACTIVATE_SCRIPT'"
    return -1
fi;
source $ACTIVATE_SCRIPT

echo "which python="$(which python 2>&1)
echo "python -v="$(python --version)

# #############################################################################
# PATH
# #############################################################################

echo "# Set path"

export PATH=.:$PATH

export PATH=$AMP:$PATH

export PATH=$AMP/dev_scripts:$PATH
export PATH=$AMP/dev_scripts/aws:$PATH
export PATH=$AMP/dev_scripts/git:$PATH
export PATH=$AMP/dev_scripts/infra:$PATH
export PATH=$AMP/dev_scripts/install:$PATH
export PATH=$AMP/dev_scripts/notebooks:$PATH
export PATH=$AMP/dev_scripts/testing:$PATH

# Remove duplicates.
export PATH=$(echo $PATH | perl -e 'print join(":", grep { not $seen{$_}++ } split(/:/, scalar <>))')

# Print.
echo $PATH | perl -e 'print join("\n", grep { not $seen{$_}++ } split(/:/, scalar <>))'

# #############################################################################
# PYTHONPATH
# #############################################################################

echo "# Set PYTHONPATH"
export PYTHONPATH=$PWD:$PYTHONPATH

# Remove duplicates.
export PYTHONPATH=$(echo $PYTHONPATH | perl -e 'print join(":", grep { not $seen{$_}++ } split(/:/, scalar <>))')

# Print.
echo $PYTHONPATH | perl -e 'print join("\n", grep { not $seen{$_}++ } split(/:/, scalar <>))'

# #############################################################################
# Configure environment
# #############################################################################

echo "# Configure env"
echo "which gh="$(which gh)

# Select which profile to use by default.
export AM_AWS_PROFILE="am"

# These variables are propagated to Docker.
export AM_ECR_BASE_PATH="665840871993.dkr.ecr.us-east-1.amazonaws.com"
export AM_S3_BUCKET="alphamatic-data"

# Print the AM env vars.
printenv | egrep "AM_|AWS_" | sort

# `invoke` doesn't seem to allow to have a single configuration file and
# doesn't allow to specify it through an env var, so we create an alias.
# From https://github.com/pyinvoke/invoke/issues/543
# This doesn't work:
# > export INVOKE_RUNTIME_CONFIG=$(pwd)/invoke.yaml
#
# These don't work:
# > export INVOKE_TASKS_AUTO_DASH_NAMES=0
# > export INVOKE_RUN_ECHO=1
#
# This works:
# > export INVOKE_DEBUG=1
#
# This doesn't work:
# > INVOKE_OPTS="--config $(pwd)/invoke.yaml"
# > alias invoke="invoke $INVOKE_OPTS"
alias i="invoke"
alias it="invoke traceback"
alias itpb="pbpaste | traceback_to_cfile.py -i - -o cfile"
alias ih="invoke --help"
alias il="invoke --list"

# Print the aliases.
alias

if [[ $(whoami) == "saggese" && $(git remote -v) =~ "cmamp.git" ]]; then
    export GIT_SSH_COMMAND="ssh -i ~/.ssh/cryptomatic/id_rsa.cryptomtc.github"
    echo "GIT_SSH_COMMAND=$GIT_SSH_COMMAND"
fi;

echo "==> SUCCESS <=="
