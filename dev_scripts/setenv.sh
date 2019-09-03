#!/bin/bash -e

# This file needs to be sourced:
#   > source setenv.sh
# instead of executed:
#   > ./setenv.sh
# in order to set the env vars in the calling shell.

# TODO(gp): Convert into python if we find a way to change the env dirs in the
# calling shell.

# TODO(gp): Use git to make sure you are in the root of the repo.
#  echo "Error: you need to be in the root of the repo to configure the environment"

ENV_NAME="develop"

# Needed for getopts to work when sourcing the script.
OPTIND=1
while getopts "t:" option
  do
    case ${option}
    in
      t )
        ENV_NAME=$OPTARG
        ;;
      \? )
        echo "Usage: source dev_scripts/setenv.sh [-h] [-t CONDA_ENV_NAME]"
        return -1
        ;;
  esac
  shift $((OPTIND -1))
done
extra="$1"
if [[ $extra != "bell-style" && $extra != "" ]]; then
  echo "Error: can't parse '$extra'"
  return -1
fi;
echo "ENV_NAME=$ENV_NAME"

echo "#############################################################################"
echo "# Config git"
echo "#############################################################################"

user_name=$(whoami)
echo "user_name='$user_name'"
# TODO(gp): Generalize this to all users.
if [[ $user_name == "saggese" ]]; then
  git config --local user.email "saggese@gmail.com"
  git config --global user.name "saggese"
elif [[ $user_name == "paul" ]]; then
  git config --local user.email "smith.paul.anthony@gmail.com"
  git config --global user.name "paul"
elif [[ $user_name == "jenkins" ]]; then
  echo "There is no need to setup git for user '$user_name'."
else
  echo "Error: Invalid user '$user_name'. Add your credentials to setenv.sh"
  return -1
fi;

if [[ $user_name != "jenkins" ]]; then
  git config --list | \grep $user_name
else
  git config --list
fi;

echo "#############################################################################"
echo "# Config python"
echo "#############################################################################"
# Disable python code caching.
export PYTHONDONTWRITEBYTECODE=x

# Python packages.
export PYTHONPATH=""
export PYTHONPATH=$PYTHONPATH:$(pwd)
# Remove redundant paths.
PYTHONPATH="$(echo $PYTHONPATH | perl -e 'print join(":", grep { not $seen{$_}++ } split(/:/, scalar <>))')"
echo "PYTHONPATH=$PYTHONPATH"

echo "#############################################################################"
echo "# Config conda"
echo "#############################################################################"
UTIL_DIR=$SRC_DIR/amp
# TODO(gp): Add a loop picking up all the dirs.
export PATH=$UTIL_DIR/dev_scripts:$UTIL_DIR/ipynb_scripts:$UTIL_DIR/install:$UTIL_DIR/aws:$PATH
export PYTHONPATH=$PYTHONPATH:$UTIL_DIR

CONDA_SCRIPT_NAME="$UTIL_DIR/helpers/get_conda_config_path.py"
if [[ ! -e $CONDA_SCRIPT_NAME ]]; then
  echo "Can't find file $CONDA_SCRIPT_NAME"
  return -1
fi;

CONDA_CONFIG_PATH=$($CONDA_SCRIPT_NAME)
echo "CONDA_CONFIG_PATH=$CONDA_CONFIG_PATH"
source $CONDA_CONFIG_PATH

echo "CONDA_PATH="$(which conda)
conda info --envs
conda activate $ENV_NAME
conda info --envs

# Workaround for conda error not finding certain installed packages.
if [[ 0 == 1 ]]; then
  CONDA_LIBS=$(conda info | \grep "active env location" | awk '{print $NF;}')
  echo "CONDA_LIBS=$CONDA_LIBS"
  export PYTHONPATH=$PYTHONPATH:$CONDA_LIBS/lib/python2.7/site-packages
  # To fix: python -c "import configparser"
  #export PYTHONPATH=$PYTHONPATH:$CONDA_LIBS/lib/python2.7/site-packages/backports
fi;

# Remove redundant paths.
PATH="$(echo $PATH| perl -e 'print join(":", grep { not $seen{$_}++ } split(/:/, scalar <>))')"
# TODO(gp): Print a better list.
echo "PATH=$PATH"

if [ 0 == 1 ]; then
  echo "#############################################################################"
  echo "# Fixing pandas warnings."
  echo "#############################################################################"
  FILE_NAME="dev_scripts/fix_pandas_numpy_warnings.py"
  if [ -f $FILE_NAME ]; then
    $FILE_NAME
  fi;
fi;

# AWS.
export AM_INST_ID="i-07f9b5323aa7a2ff2"

echo "#############################################################################"
echo "# Testing packages"
echo "#############################################################################"
$UTIL_DIR/install/package_tester.py
