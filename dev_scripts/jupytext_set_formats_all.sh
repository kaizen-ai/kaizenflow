#!/bin/bash

# Refresh all the notebooks

find . -name "*.ipynb" | grep -v ipynb_checkpoints | xargs -t -L 1 jupytext --sync --to py:percent
