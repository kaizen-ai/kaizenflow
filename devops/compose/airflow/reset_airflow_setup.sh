#!/bin/bash -xe

# Reset the DB.
docker exec \
    -ti airflow_cont \
    airflow db reset && \
    echo "Reset airflow DB"
