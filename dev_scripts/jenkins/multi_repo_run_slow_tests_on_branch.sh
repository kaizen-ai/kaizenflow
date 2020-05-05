set +e
dev_scripts/jenkins/amp.run_slow_tests.sh
printf "$?" > ./tmp_exit_status.txt