#!/bin/bash -xe

replace_text.py \
  --old "(?<!def\s)(\btimestamp_as_str_to_timestamp\b)" \
  --new "str_to_timestamp" \
  --exclude_files "dev_scripts/cleanup_scripts/SorrTask584_Use_str_to_timestamp_instead_of_timestamp_as_str_to_timestamp.sh"


replace_text.py \
  --old "omreconc.timestamp_as_str_to_timestamp" \
  --new "hdateti.str_to_timestamp" \
  --exclude_files "dev_scripts/cleanup_scripts/SorrTask584_Use_str_to_timestamp_instead_of_timestamp_as_str_to_timestamp.sh"
 