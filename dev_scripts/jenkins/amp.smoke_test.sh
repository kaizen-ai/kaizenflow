#!/bin/bash -e

# """
# - Run all the Jenkins builds locally to debug.
# - To run
#   > (cd $HOME/src/commodity_research1/amp; dev_scripts/jenkins/smoke_test.sh 2>&1 | tee log.txt)
# """

function frame() {
  echo "********************************************************************"
  echo "$*"
  echo "********************************************************************"
}

function execute() {
  frame "$*"
  eval $*
}

CMD="dev_scripts/jenkins/amp.build_clean_env.amp_develop.sh"
execute $CMD

# This modifies the client, so we disable it by default.
if [[ 0 == 1 ]]; then
    $CMD="dev_scripts/jenkins/amp.run_pytest_collect.run_linter.sh"
    execute $CMD
fi;

CMD="dev_scripts/jenkins/amp.run_fast_tests.sh"
execute $CMD

CMD="dev_scripts/jenkins/amp.run_parallel_fast_tests.sh"
execute $CMD

CMD="dev_scripts/jenkins/amp.build_clean_env.run_fast_coverage_tests.sh"
execute $CMD

CMD="dev_scripts/jenkins/amp.build_clean_env.run_fast_tests.sh"
execute $CMD

CMD="dev_scripts/jenkins/amp.build_clean_env.run_slow_coverage_tests.sh"
execute $CMD
