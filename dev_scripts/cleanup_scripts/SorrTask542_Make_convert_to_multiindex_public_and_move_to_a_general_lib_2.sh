#!/bin/bash -xe

file_names="dev_scripts/cleanup_scripts/SorrTask542_Make_convert_to_multiindex_public_and_move_to_a_general_lib.sh"

replace_text.py \
  --old "(?<!\.)(_convert_to_multiindex)" \
  --new "dtfcorutil.convert_to_multiindex" \
  --exclude_files "$file_names" \
  --ext "py,ipynb" \

replace_text.py \
  --old "dtfsysonod._convert_to_multiindex" \
  --new "dtfcorutil.convert_to_multiindex" \
  --exclude_files "$file_names" \
  --ext "py,ipynb" \

replace_text.py \
  --old "dtfsys.source_nodes._convert_to_multiindex" \
  --new "dtfcorutil.convert_to_multiindex" \
  --exclude_files "$file_names" \
  --ext "py,ipynb" \