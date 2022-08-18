#!/bin/bash -xe

#if [[ 0 == 1 ]]; then
    # This script should be invoked on a different repo so that we don't annihilate
    # this file.
    #SOURCE_REPO=CMTask974_run_script
    #TARGET_REPO=/Users/saggese/src/cmamp2
    #cd $TARGET_REPO

    # Make sure that the source and target branch are at master.
    #if [[ 0 == 1 ]]; then
    #  git pull
    #  i git_merge_master
    #  git push --force
    #fi

    # Create the branch with the changes.
    #TARGET_BRANCH=CmTask1074_numeric_to_numerical_1
    #i git_create_branch -b $TARGET_BRANCH
    #git checkout -B $TARGET_BRANCH

    # Clean up.
    #git reset --hard origin/$TARGET_BRANCH

    # Apply manual changes.
    #git merge --no-commit origin/$SOURCE_REPO
#fi;

./dev_scripts/replace_text.py \
  --old "cconfig.get_config_from_nested_dict" \
  --new "cconfig.Config.from_dict" \

./dev_scripts/replace_text.py \
  --old "cfgb.get_config_from_nested_dict" \
  --new "cconfig.Config.from_dict" \

./dev_scripts/replace_text.py \
  --old "ccocouti.get_config_from_nested_dict" \
  --new "cconfig.Config.from_dict" \

# Remove unused imports from affected files.
invoke lint -m --only-format
