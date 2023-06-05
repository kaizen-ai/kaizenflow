#!/bin/bash -xe

script_name="cmamp/dev_scripts/cleanup_scripts/rename_pytestraises.sh"


/cmamp/dev_scripts/replace_text.py \
    --old "self.assertRaises" \
    --new "self.assertRaises" \
    --exclude_files $script_name \

### Remove unused imports from affected files.
##invoke lint -m --only-format


