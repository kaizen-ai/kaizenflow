#!/bin/bash -xe

dir_names="amp/dev_scripts/cleanup_scripts dev_scripts/cleanup_scripts"

replace_text.py \
    --old "SimulatedBroker" \
    --new "DataFrameBroker" \
    --exclude_dirs "$dir_names"
