#!/bin/bash
cmd="pytest $* 2>&1 | tee tmp.pytest_script.log"
echo "> $cmd"
eval $cmd
