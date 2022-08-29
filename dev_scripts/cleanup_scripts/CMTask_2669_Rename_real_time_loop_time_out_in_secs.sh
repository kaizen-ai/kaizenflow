#!/bin/bash -xe

script_name="dev_scripts/cleanup_scripts/CMTask_2669_Rename_real_time_loop_time_out_in_secs.sh"

replace_text.py \
  --old "real_time_loop_time_out_in_secs: Optional[int]" \
  --new "rt_timeout_in_secs_or_time: Optional[Union[int, datetime.time]]" \
  --exclude_files $script_name 

replace_text.py \
  --old "real_time_loop_time_out_in_secs" \
  --new "rt_timeout_in_secs_or_time" \
  --exclude_files $script_name 
