#!/bin/bash -xe

dir_names="dev_scripts/cleanup_scripts dataflow/system dataflow/system/test"

replace_text.py \
  --old "ForecastSystem" \
  --new "NonTime_ForecastSystem" \
  --exclude_dirs "$dir_names" 

replace_text.py \
  --old "Time_NonTime_ForecastSystem" \
  --new "Time_ForecastSystem" \
  --exclude_dirs "$dir_names" 
