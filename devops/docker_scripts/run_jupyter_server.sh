#!/usr/bin/env bash

set -e
source ~/.bash_profile

cmd="jupyter notebook --ip=* --port=${PORT} --allow-root"
echo "> cmd=$cmd"
eval $cmd
