# Make output white.
echo -e '\033[0;0m\c'

function execute() {
  echo "+ $*"
  eval $*
}

function frame() {
  echo "####################################################################"
  echo "$*"
  echo "####################################################################"
}

ACK_OPTS="--smart-case --nogroup --nocolor"

GIT_LOG_OPTS='%Creset%Cgreen%h %C(reset)%C(cyan)%<(8)%aN%Creset %Creset%C(bold white) %<(55)%s %C(bold black)(%>(14)%ar) %C(red)%ad %C(yellow)%<(10)%d%C(reset)'


# ##############################################################################

function get_python_version() {
  python - <<END
import sys
pyv = sys.version_info[:]
pyv_as_str = ".".join(map(str, pyv[:3]))
print("python version='%s'" % pyv_as_str)
is_too_old = pyv_as_str < "3.6"
print("is_too_old=%s" % is_too_old)
sys.exit(is_too_old);
END
  return $?
}


function execute_setenv() {
  if [[ -z $1 ]]; then
    echo "ERROR: Need to specify a parameter representing setenv"
    return 1
  fi;
  SETENV=$1
  if [[ -z $2 ]]; then
    echo "ERROR: Need to specify the env to use"
    return 1
  fi;
  ENV_NAME=$2
  # Create the script to execute calling python.
  echo "Creating setenv script ... done"
  DATETIME=$(date "+%Y%m%d-%H%M%S")_$(python -c "import time; print(int(time.time()*1000))")
  #DATETIME=""
  SCRIPT_FILE=/tmp/$SETENV.${DATETIME}.sh
  echo "SCRIPT_FILE=$SCRIPT_FILE"
  cmd="$EXEC_PATH/$SETENV.py --output_file $SCRIPT_FILE -e $ENV_NAME"
  execute $cmd
  echo "Creating setenv script '$SCRIPT_FILE' ... done"

  # Execute the newly generated script.
  echo "Sourcing '$SCRIPT_FILE' ..."
  source $SCRIPT_FILE
  echo "Sourcing '$SCRIPT_FILE' ... done"

  echo "Running '$EXEC_NAME' ... done"
}
