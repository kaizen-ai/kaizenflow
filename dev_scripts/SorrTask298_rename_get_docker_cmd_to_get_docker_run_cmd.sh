#! /bin/bash -xe

# Change the directory to the root
# In Juypter note the root is /app
dir_names="/Users/leosmacbook/Desktop/Sorretum_Intern"

replace_text.py \
    --old "_get_docker_run_cmd" \
    --new "_get_docker_run_cmd" \
    --dst_dir "$dir_names"




