#!/bin/bash -xe

script_name="amp/dev_scripts/cleanup_scripts/CMTask2746_Move_every_test_case.sh"
# dir="amp/im_v2/common/data/client/"

# replace_text.py \
#   --old "import im_v2.common.data.client.test.im_client_test_case as icdctictc" \
#   --new "import im_v2.common.data.client.im_client_test_case as imvcdcimctc" \
#   --exclude_files $script_name \

# replace_text.py \
#   --old "icdctictc." \
#   --new "imvcdcimctc." \
#   --exclude_files $script_name\

# replace_text.py \
#   --old "import im_v2.common.data.client.im_client_test_case as imvcdcimctc" \
#   --new "import im_v2.common.data.client as icdc" \
#   --exclude_files $dir\

# replace_text.py \
#   --old "imvcdcimctc." \
#   --new "icdc." \
#   --exclude_files $dir\

dir="amp/market_data/"

# replace_text.py \
#   --old "import market_data.test.market_data_test_case as mdtmdtca" \
#   --new "import market_data.market_data_test_case as mdmdteca" \
#   --exclude_files $script_name \

# replace_text.py \
#   --old "mdtmdtca." \
#   --new "mdmdteca." \
#   --exclude_files $script_name\

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
