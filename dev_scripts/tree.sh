#!/bin/bash -e
git clean -fd
find . -name "__pycache__" -o -name "tmp.build" -o -name ".ipynb_checkpoints" | xargs rm -rf
tree --dirsfirst -n -F --charset unicode $@
