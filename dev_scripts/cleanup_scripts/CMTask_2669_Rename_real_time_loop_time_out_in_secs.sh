#!/bin/bash -xe

script_name="amp/dev_scripts/cleanup_scripts/CMTask_2669_Rename_real_time_loop_time_out_in_secs.sh"

replace_text.py \
  --old "real_time_loop_time_out_in_secs: Optional[int]" \
  --new "rt_timeout_in_secs_or_time: Optional[Union[int, datetime.time]]" \
  --exclude_files $script_name 

replace_text.py \
  --old "real_time_loop_time_out_in_secs" \
  --new "rt_timeout_in_secs_or_time" \
  --exclude_files $script_name 

replace_text.py \
  --old "time_out_in_secs: int" \
  --new "rt_timeout_in_secs_or_time: Optional[Union[int, datetime.time]]" \
  --only_dirs "core dataflow helpers dataflow_amp" \
  --exclude_files $script_name 

replace_text.py \
  --old "time_out_in_secs" \
  --new "rt_timeout_in_secs_or_time" \
  --only_dirs "core dataflow helpers dataflow_amp" \
  --exclude_files $script_name 
