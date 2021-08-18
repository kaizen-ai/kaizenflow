#!/bin/bash
file_name="tmp.pytest_script.log"
cmd="pytest $* 2>&1 | tee $file_name"
echo "> $cmd"
eval $cmd

echo "# pytest.sh: Saved result in '$file_name'"
ls -l $file_name
