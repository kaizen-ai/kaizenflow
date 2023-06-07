#!/bin/bash -xe

#script_name="cmamp/dev_scripts/cleanup_scripts/rename_pytestraises.sh"
dir_names="/cmamp/"


/cmamp/dev_scripts/replace_text.py \
    --old "pytest.raises" \
    --new "self.assertRaises" \
    --preview \
    --only_dirs "$dir_names"    
    


