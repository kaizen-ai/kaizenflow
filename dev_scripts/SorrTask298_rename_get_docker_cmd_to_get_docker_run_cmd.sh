#! /bin/bash -xe


dir_names="/app"

replace_text.py \
    --old "_get_docker_run_cmd" \
    --new "_get_docker_run_cmd" \
    --dst_dir "$dir_names"

# + language="sh"
# cat rename_get_docker_run_cmd.sh 
# ./rename_get_docker_run_cmd.sh
# -


