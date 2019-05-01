#!/bin/bash -xe

# Get input jupyter notebook file name
f_ipy=$1

# Convert to .py
jupyter nbconvert --to python $f_ipy

# Strip out magic lines
f_py="${f_ipy%.*}".py
#f2_py="${f_ipy%.*}2".py
python dev_scripts/strip_ipython_magic.py $f_py
