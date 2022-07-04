#!/bin/bash -xe
sudo apt install universal-ctags

rm tags
ctags --languages=Python --exclude=.git --exclude=.mypy_cache -R .
