#!/bin/bash -xe

  script_name="dev_scripts/cleanup_scripts/CMTask2669_Rename_initial_replayed_delay.sh"

  replace_text.py \
    --old "real_time_loop_time_out_in_secs: int" \
    --new "rt_timeout_in_secs_or_timestamp: Union[int, pd.Timestamp]" \
    --exclude_files $script_name \

  replace_text.py \
    --old "real_time_loop_time_out_in_secs" \
    --new "rt_timeout_in_secs_or_timestamp" \
    --exclude_files $script_name \
