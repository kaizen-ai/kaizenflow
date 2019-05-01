#!/bin/bash -e

DIR_NAME=$(jupyter --data-dir)/nbextensions
echo "DIR_NAME=$DIR_NAME"

# Create required directory in case (optional)
mkdir -p $DIR_NAME
# Clone the repository
cd $DIR_NAME
git clone https://github.com/lambdalisue/jupyter-vim-binding vim_binding
# Activate the extension
jupyter nbextension enable vim_binding/vim_binding
