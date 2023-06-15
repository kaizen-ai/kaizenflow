#!/bin/bash -xe

curr_file="/amp/dev_scripts/cleanup_scripts dev_scripts/cleanup_scripts"

replace_text.py \
    --old "_get_docker_cmd" \
    --new "_get_docker_run_cmd" \
    --exclude_dirs "$curr_file"




