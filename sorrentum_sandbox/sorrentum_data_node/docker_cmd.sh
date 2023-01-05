#!/bin/bash -xe

docker exec \
    -ti airflow_cont \
    $@
