#!/bin/bash -xe

# We want to apply changes to the code base 99% automatically
# 1) Prepare one PR with
#    a) the code that needs to be changed manually
#       - E.g., in this case merge `dataframe_to_str` and `df_to_short_str`
#         from `hpandas` into `df_to_str`
#    b) more contextual changes (e.g., adding unit tests)
#    c) the script for the replacement of the caller
#       `CmTask972_Merge_dataframe_to_str_and_df_to_short_str_into_hpandas.py`
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

# Replace function calls.
./dev_scripts/cleanup_scripts/CmTask972_Merge_dataframe_to_str_and_df_to_short_str_into_hpandas.py

# Apply lint to affected files.
invoke lint -m --only-format
