#!/bin/bash -e

source helpers.sh

msg="$@"
echo "msg='$msg'"
if [[ -z $msg ]]; then
  echo "Need a commit message"
  exit -1
fi;

cd amp
execute "git commit -am '$msg'"
gp

cd ..
execute "git submodule update --remote"
execute "git add amp"

execute "git commit -am '$msg'"
gp
