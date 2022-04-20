#!/bin/bash -xe

if [[ 0 == 1 ]]; then
    # This script should be invoked on a different repo so that we don't annihilate
    # this file.
    SOURCE_REPO=CMTask1206_run_script
    TARGET_REPO=/Users/saggese/src/cmamp2
    cd $TARGET_REPO

    # Make sure that the source and target branch are at master.
    #if [[ 0 == 1 ]]; then
    #  git pull
    #  i git_merge_master
    #  git push --force
    #fi

    # Create the branch with the changes.
    TARGET_BRANCH=CMTask1292_ck_profile_authentication_2
    #i git_create_branch -b $TARGET_BRANCH
    git checkout -B $TARGET_BRANCH

    # Clean up.
    git reset --hard origin/$TARGET_BRANCH

    # Apply manual changes.
    git merge --no-commit origin/$SOURCE_REPO
fi;

# Replace old usages of AWS variables in all files.
./dev_scripts/replace_text.py \
  --old "AM_S3_BUCKET" \
  --new "AM_AWS_S3_BUCKET"

./dev_scripts/replace_text.py \
  --old "AWS_ACCESS_KEY_ID" \
  --new "AM_AWS_ACCESS_KEY_ID"

./dev_scripts/replace_text.py \
  --old "AWS_SECRET_ACCESS_KEY" \
  --new "AM_AWS_SECRET_ACCESS_KEY"

./dev_scripts/replace_text.py \
  --old "AWS_DEFAULT_REGION" \
  --new "AM_AWS_DEFAULT_REGION"

# Remove unused imports from affected files.
invoke lint -m --only-format
