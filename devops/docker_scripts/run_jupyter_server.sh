#!/usr/bin/env bash

set -e

cmd="jupyter notebook --ip=* --port=${PORT} --allow-root"
echo "> cmd=$cmd"
eval $cmd
