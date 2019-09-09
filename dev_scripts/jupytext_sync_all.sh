#!/bin/bash

# Sync all the notebooks.

find . -name "*.ipynb" | grep -v ipynb_checkpoints | xargs -t -L 1 jupytext --sync --update --to py:percent
