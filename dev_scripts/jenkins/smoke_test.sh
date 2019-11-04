#!/bin/bash -xe
dev_scripts/jenkins/run_pytest_collect.run_linter.sh

dev_scripts/jenkins/build_clean_env.amp_develop.sh
dev_scripts/jenkins/run_fast_tests.sh
dev_scripts/jenkins/run_parallel_fast_tests.sh

dev_scripts/jenkins/build_clean_env.run_fast_coverage_tests.sh
dev_scripts/jenkins/build_clean_env.run_fast_tests.sh
dev_scripts/jenkins/build_clean_env.run_slow_coverage_tests.sh
