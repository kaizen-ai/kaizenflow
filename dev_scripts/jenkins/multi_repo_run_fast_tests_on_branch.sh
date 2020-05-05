set +e
dev_scripts/jenkins/amp.run_fast_tests.sh
printf "$?" > ./tmp_exit_status.txt