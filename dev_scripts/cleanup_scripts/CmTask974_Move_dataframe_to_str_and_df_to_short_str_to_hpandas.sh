#!/bin/bash -xe

# This script should be invoked on a different repo so that we don't annihilate
# this file.
TARGET_REPO=/Users/saggese/src/cmamp2
cd $TARGET_REPO

# Clean up.
git pull
git reset --hard HEAD

# Apply changes.
git merge origin/CmTask972_Merge_dataframe_to_str_and_df_to_short_str_into_hpandas

# Move dataframe_to_str from hprint to hpandas.
./dev_scripts/replace_text.py \
  --old "hprint.dataframe_to_str" \
  --new "hpandas.dataframe_to_str" \
  --ext "py"

# Move df_to_short_str from hprint to hpandas.
./dev_scripts/replace_text.py \
  --old "hprint.df_to_short_str" \
  --new "hpandas.df_to_short_str" \
  --ext "py"

# Add potential missing imports.
./dev_scripts/replace_text.py \
  --old "import helpers.hprint as hprint" \
  --new "import helpers.hpandas as hpandas; import helpers.hprint as hprint" \
  --ext "py"

# Remove unused imports from the changed files.
#invoke lint --files "$(find . -name '*.py')" --phases="amp_isort autoflake"
invoke lint -m --phases="amp_isort autoflake"
