#!/bin/bash -xe

script_name="dev_scripts/cleanup_scripts/CMTask2669_Rename_sleep_interval_in_secs.sh"

replace_text.py \
  --old "sleep_interval_in_secs: float" \
  --new "bar_duration_in_secs: int" \
  --exclude_files $script_name \

replace_text.py \
  --old "sleep_interval_in_secs" \
  --new "bar_duration_in_secs" \
  --exclude_files $script_name \

