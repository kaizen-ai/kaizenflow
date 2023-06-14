#!/bin/bash -xe

dir_name="/app/"
script_name="/app/dev_scripts/cleanup_scripts/Rename_to_multi_line_cmd.sh"

/app/dev_scripts/replace_text.py \
   --old "_to_multi_line_cmd" \
   --new "to_multi_line_cmd" \
   --only_dirs "$dir_name"  \
   --exclude_files "$script_name"
