#!/bin/bash -xe

if [[ 0 == 1 ]]; then
# We want to apply changes to the code base 99% automatically
# 1) Prepare one PR with
#    a) the code that needs to be changed manually
#       - E.g., in this case move the code from hprint to hpandas
#    b) more contextual changes (e.g., adding unit tests)
#    c) the script for the replacement of the caller (e.g., `replace_text.py or
#       a custom one like this one), like this one
#       - Conceptually the script
#         - prepares the target Git client
#         - merge this branch with the manual changes
#         - make the automated changes
#
# 2) Run from scratch this script getting the regression to pass
#
# 3) Do a PR
#
# 4) The reviewer does a `git checkout` of this PR and runs the script
#    - Review carefully the changes to make sure we are not screwing things up
#    - Run the regressions
#    - Merge

# This script should be invoked on a different repo so that we don't annihilate
# this file.
SOURCE_REPO=CMTask974_run_script
TARGET_REPO=/Users/saggese/src/cmamp2
cd $TARGET_REPO

# Make sure that the source and target branch are at master.
#if [[ 0 == 1 ]]; then
#  git pull
#  i git_merge_master
#  git push --force
#fi

# Create the branch with the changes.
TARGET_BRANCH=CmTask972_Merge_dataframe_to_str_and_df_to_short_str_into_hpandas_2
#i git_create_branch -b $TARGET_BRANCH
git checkout -B $TARGET_BRANCH

# Clean up.
git reset --hard origin/$TARGET_BRANCH

# Apply manual changes.
git merge --no-commit origin/$SOURCE_REPO
fi;

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
