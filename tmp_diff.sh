#!/bin/bash
if [[ $1 == "wrap" ]]; then
    cmd='vimdiff -c "windo set wrap"'
else
    cmd='vimdiff'
fi;
cmd="$cmd helpers/test/outcomes/TestDryRunTasks1.test_docker_ps/tmp.final.actual.txt helpers/test/outcomes/TestDryRunTasks1.test_docker_ps/tmp.final.expected.txt"
eval $cmd
