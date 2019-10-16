# Make output white.
echo -e '\033[0;0m'


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


GIT_LOG_OPTS='%Creset%Cgreen%h %C(reset)%C(cyan)%<(8)%aN%Creset %Creset%C(bold white) %<(55)%s %C(bold black)(%>(14)%ar) %C(red)%ad %C(yellow)%<(10)%d%C(reset)'
