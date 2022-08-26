#!/bin/bash -xe

  script_name="dev_scripts/cleanup_scripts/CMTask2669_Rename_initial_replayed_dt.sh"

  replace_text.py \
    --old "initial_timestamp" \
    --new "initial_replayed_timestamp" \
    --exclude_files $script_name \
