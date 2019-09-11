#!/bin/bash -xe

# This file needs to be sourced:
#   > source setenv.sh
# instead of executed:
#   > ./setenv.sh
# in order to set the env vars in the calling shell.

EXEC_NAME="${BASH_SOURCE[0]}"
#echo $EXEC_NAME
DIR=$(dirname "$EXEC_NAME")
EXEC_PATH=$(cd $DIR ; pwd -P)

DATETIME=$(date "+%Y%m%d-%H%M%S")
SCRIPT_FILE=/tmp/setenv.${DATETIME}.sh

$EXEC_PATH/_setenv.py $* >$SCRIPT_FILE


#  echo "Error: Invalid user '$USER_NAME'. Add your credentials to setenv.sh"
#  return -1

source $SCRIPT_FILE

#eval($SCRIPT_BODY)

## TODO(gp): Convert into python if we find a way to change the env dirs in the
## calling shell.
#
## TODO(gp): Use git to make sure you are in the root of the repo.
##  echo "Error: you need to be in the root of the repo to configure the environment"
#
#EXEC_NAME="${BASH_SOURCE[0]}"
#echo "# Executing '$EXEC_NAME' ..."
#DIR=$(dirname "$EXEC_NAME")
#EXEC_PATH="$(cd $DIR ; pwd -P)"
#
#CURR_DIR=$(dirname "$EXEC_PATH")
#echo "CURR_DIR=$CURR_DIR"
#if [[ -z $CURR_DIR ]]; then
#  echo "Error"
#  return -1
#fi;
#
#ENV_NAME="develop"
#
## Needed for getopts to work when sourcing the script.
#OPTIND=1
#while getopts "t:" option
#  do
#    case ${option}
#    in
#      t )
#        ENV_NAME=$OPTARG
#        ;;
#      \? )
#        echo "Usage: source dev_scripts/setenv.sh [-h] [-t CONDA_ENV_NAME]"
#        return -1
#        ;;
#  esac
#  shift $((OPTIND -1))
#done
#EXTRA="$1"
#if [[ $EXTRA != "bell-style" && $EXTRA != "" ]]; then
#  echo "Error: can't parse '$EXTRA'"
#  return -1
#fi;
#echo "ENV_NAME=$ENV_NAME"
#
#echo "#############################################################################"
#echo "# Config git"
#echo "#############################################################################"
#
#USER_NAME=$(whoami)
#echo "USER_NAME='$USER_NAME'"
## TODO(gp): Generalize this to all users.
#if [[ $USER_NAME == "saggese" || $USER_NAME == "gp" ]]; then
#  git config --local user.email "saggese@gmail.com"
#  git config --global user.name "saggese"
#elif [[ $USER_NAME == "paul" ]]; then
#  git config --local user.email "smith.paul.anthony@gmail.com"
#  git config --global user.name "paul"
#elif [[ $user_name == "julia" ]]; then
#  git config --local user.email "julia@particle.one"
#  git config --global user.name "Julia"
#elif [[ $USER_NAME == "jenkins" ]]; then
#    echo "There is no need to setup git for user '$USER_NAME'."
#else
#  echo "Error: Invalid user '$USER_NAME'. Add your credentials to setenv.sh"
#  return -1
#fi;
#
#if [[ $USER_NAME != "jenkins" ]]; then
#  git config --list | \grep $USER_NAME
#else
#  git config --list
#fi;
#
#echo "#############################################################################"
#echo "# Config python"
#echo "#############################################################################"
## Disable python code caching.
#export PYTHONDONTWRITEBYTECODE=x
#
## Python packages.
#
##export PYTHONPATH=""
#export PYTHONPATH=$CURR_DIR:$PYTHONPATH
#
## Remove redundant paths.
#PYTHONPATH="$(echo $PYTHONPATH | $EXEC_PATH/remove_redundant_paths.sh)"
#echo "PYTHONPATH=$PYTHONPATH"
#echo $PYTHONPATH | $EXEC_PATH/print_paths.sh
#
#echo "#############################################################################"
#echo "# Config conda"
#echo "#############################################################################"
#
#CONDA_SCRIPT_NAME="$CURR_DIR/helpers/get_conda_config_path.py"
#if [[ ! -e $CONDA_SCRIPT_NAME ]]; then
#  echo "Can't find file $CONDA_SCRIPT_NAME"
#  return -1
#fi;
#
#CONDA_CONFIG_PATH=$($CONDA_SCRIPT_NAME)
#echo "CONDA_CONFIG_PATH=$CONDA_CONFIG_PATH"
#source $CONDA_CONFIG_PATH
#
#echo "CONDA_PATH="$(which conda)
#conda info --envs
#conda activate $ENV_NAME
#conda info --envs
#
## Workaround for conda error not finding certain installed packages.
#if [[ 0 == 1 ]]; then
#  CONDA_LIBS=$(conda info | \grep "active env location" | awk '{print $NF;}')
#  echo "CONDA_LIBS=$CONDA_LIBS"
#  export PYTHONPATH=$CONDA_LIBS/lib/python2.7/site-packages:$PYTHONPATH
#  # To fix: python -c "import configparser"
#  #export PYTHONPATH=$CONDA_LIBS/lib/python2.7/site-packages/backports:$PYTHONPATH
#fi;
#
#if [ 0 == 1 ]; then
#  echo "#############################################################################"
#  echo "# Fixing pandas warnings."
#  echo "#############################################################################"
#  FILE_NAME="dev_scripts/fix_pandas_numpy_warnings.py"
#  if [ -f $FILE_NAME ]; then
#    $FILE_NAME
#  fi;
#fi;
#
#echo "#############################################################################"
#echo "# Config bash"
#echo "#############################################################################"
#
## TODO(gp): Add a loop picking up all the dirs.
#export PATH=$CURR_DIR/dev_scripts:$CURR_DIR/ipynb_scripts:$CURR_DIR/install:$CURR_DIR/aws:$PATH
#
## Remove redundant paths.
#PATH="$(echo $PATH | $EXEC_PATH/remove_redundant_paths.sh)"
#echo "PATH=$PATH"
#echo $PATH | $EXEC_PATH/print_paths.sh
#
#echo "#############################################################################"
#echo "# Testing packages"
#echo "#############################################################################"
#$CURR_DIR/install/package_tester.py
#
#echo "# ... done executing '$EXEC_PATH'"
