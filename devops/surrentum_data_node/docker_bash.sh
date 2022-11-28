#!/bin/bash -xe
# Execute bash in the Airflow container.
docker exec \
    -ti airflow_cont \
    bash
