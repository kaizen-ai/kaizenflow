#!/bin/bash -xe

dir_names="/app/"
script_name="/app/dev_scripts/cleanup_scripts/SorrTask258_Replace_pytest_raises_with_self_attest_raises.sh"

/app/dev_scripts/replace_text.py \
	--old "pytest.raises" \
    	--new "self.assertRaises" \
        --only_dirs "$dir_names" \
	--exclude_files "$script_name"
