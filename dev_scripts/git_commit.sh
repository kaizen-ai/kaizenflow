#!/bin/bash -e

source helpers.sh

msg="$@"
echo "msg='$msg'"
if [[ -z $msg ]]; then
  echo "Need a commit message"
  exit -1
fi;

cd amp
exec "git commit -am '$msg'"
gp

cd ..
exec "git submodule update --remote"
exec "git add amp"

exec "git commit -am '$msg'"
gp
