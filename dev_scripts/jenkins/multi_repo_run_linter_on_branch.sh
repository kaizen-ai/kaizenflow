set +e
source dev_scripts/jenkins/amp.run_linter_on_branch.sh
printf "${message}" > ./tmp_message.txt
printf "${exit_status}" > ./tmp_exit_status.txt