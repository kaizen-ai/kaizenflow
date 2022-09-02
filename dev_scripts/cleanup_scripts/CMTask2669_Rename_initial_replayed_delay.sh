#!/bin/bash -xe

script_name="amp/dev_scripts/cleanup_scripts/CMTask2669_Rename_initial_replayed_delay.sh"

replace_text.py \
  --old "initial_replayed_delay: int" \
  --new "replayed_delay_in_mins_or_timestamp: Union[int, pd.Timestamp]" \
  --exclude_files $script_name \

replace_text.py \
  --old "initial_replayed_delay" \
  --new "replayed_delay_in_mins_or_timestamp" \
  --exclude_files $script_name \
