#!/bin/bash -xe

dir_names="dev_scripts/cleanup_scripts dataflow/system dataflow_amp/system/mock1 dataflow/system/test dataflow_amp/system/mock1/test"

replace_text.py \
  --old "ForecastSystem" \
  --new "NonTime_ForecastSystem" \
  --exclude_dirs "$dir_names" 

replace_text.py \
  --old "ForecastSystem" \
  --new "NonTime_ForecastSystem" \
  --exclude_dirs "$dir_names" 
