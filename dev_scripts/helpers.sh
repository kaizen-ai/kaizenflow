function execute {
  echo "+ $*"
  eval $*
}


function frame {
  echo "####################################################################"
  echo "$*"
  echo "####################################################################"
}


ACK_OPTS="--smart-case --nogroup --nocolor"
