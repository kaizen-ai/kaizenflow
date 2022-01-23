#!/bin/bash -xe

# This script should be invoked on a different repo so that we don't annihilate
# this file.
#TARGET_REPO=/Users/saggese/src/cmamp2
#cd $TARGET_REPO

# Make sure that the branch is at master.
#if [[ 0 == 1 ]]; then
#  git pull
#fi

# Create the branch with the changes.
# i git_create_branch -b $TARGET_BRANCH
#TARGET_BRANCH=CmTask972_Merge_dataframe_to_str_and_df_to_short_str_into_hpandas_2
#git checkout -B $TARGET_BRANCH

# Clean up.
#git reset --hard origin/$TARGET_BRANCH

# Apply manual changes.
#git merge --no-commit origin/CmTask972_Merge_dataframe_to_str_and_df_to_short_str_into_hpandas

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

# Add imports to affected files.
FILES=$(git diff --name-only HEAD)
./dev_scripts/replace_text.py \
  --old "import helpers.hprint as hprint" \
  --new "import helpers.hpandas as hpandas; import helpers.hprint as hprint" \
  --ext "py" \
  --only_files "${FILES//$'\n'/ }" # Replace new lines with space.

# Remove unused imports from affected files.
invoke lint -m --only-format
