#!/bin/bash -xe

export JUPYTER_HOST_PORT=8888
export JUPYTER_USE_VIM=0

while getopts p:v: flag
do
    case "${flag}" in
        p) JUPYTER_HOST_PORT=${OPTARG};;
        v) JUPYTER_USE_VIM=${OPTARG};;
    esac
done
echo "JUPYTER_HOST_PORT=$JUPYTER_HOST_PORT"
echo "JUPYTER_USE_VIM=$JUPYTER_USE_VIM"

if [[ $JUPYTER_USE_VIM ]]; then
    jupyter nbextension enable vim_binding/vim_binding
fi;

jupyter-notebook --port=$JUPYTER_HOST_PORT \
    --no-browser \
    --ip=* \
    --NotebookApp.token='' --NotebookApp.password='' \
    --allow-root
