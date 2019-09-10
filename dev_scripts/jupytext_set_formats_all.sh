#!/bin/bash

# Make all the notebooks paired.
find . -name "*.ipynb" | grep -v ipynb_checkpoints | xargs -t -L 1 process_jupytext.py --action pair --file 

# Sync all the notebooks.
#find . -name "*.ipynb" | grep -v ipynb_checkpoints | xargs -t -L 1 jupytext --sync --update --to py:percent

#FILE="vendors/kibot/data_exploratory_analysis.ipynb"

# Convert a notebook into jupytext.
#jupytext --update-metadata '{"jupytext":{"formats":"ipynb,py:percent"}}' $FILE

# Test the ipynb -> py:percent -> ipynb round trip conversion
#jupytext --test --stop --to py:percent $FILE

# Convert
#jupytext --to py:percent $FILE

# Test the ipynb -> (py:percent + ipynb) -> ipynb (à la paired notebook) conversion
#jupytext --test --update --to py:percent $FILE

