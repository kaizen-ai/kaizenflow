#!/bin/bash -xe

script_name="amp/dev_scripts/cleanup_scripts/CMTask2746_Move_every_test_case.sh"

replace_text.py \
  --old "import im_v2.common.data.client.im_client_test_case as imvcdcimctc" \
  --new "import im_v2.common.data.client as icdc" \
  --exclude_files $script_name\

replace_text.py \
  --old "imvcdcimctc." \
  --new "icdc." \
  --exclude_files $script_name\

replace_text.py \
  --old "import market_data.market_data_test_case as mdmdteca" \
  --new "import market_data as mdata" \
  --exclude_files $script_name\

replace_text.py \
  --old "mdmdteca." \
  --new "mdata." \
  --exclude_files $script_name\

# Remove unused imports from affected files.
invoke lint -m --only-format
