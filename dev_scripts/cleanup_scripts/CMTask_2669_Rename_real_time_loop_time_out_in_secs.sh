#!/bin/bash -xe

  script_name="dev_scripts/cleanup_scripts/CMTask2669_Rename_initial_replayed_delay.sh"

  replace_text.py \
    --old "rt_time_out_in_secs_or_timestamp: Union[int, pd.Timestamp]" \
    --new "rt_time_out_in_secs_or_timestamp: Union[int, pd.Timestamp]" \
    --exclude_files $script_name \

  replace_text.py \
    --old "rt_time_out_in_secs_or_timestamp" \
    --new "rt_time_out_in_secs_or_timestamp" \
    --exclude_files $script_name \
