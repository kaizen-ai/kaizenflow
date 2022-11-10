#!/bin/sh

# WARNING: Run this script only during initial airflow db setup.

AIRFLOW_USER=airflow
AIRFLOW_PASSWORD=airflow
AIRFLOW_USER_EMAIL=airflow@airflow.com

# Do not load examples.
AIRFLOW__CORE__LOAD_EXAMPLES=False

echo "Initializing Airflow DB setup and Admin user setup because value of IS_INITDB is $IS_INITDB"
echo " Airflow admin username will be $AIRFLOW_USER"
# Execute 
docker exec \
    --env AIRFLOW__CORE__LOAD_EXAMPLES=False \
    -ti airflow_cont \
    airflow db init && \
    echo "Initialized airflow DB"
docker exec \
  -ti \
  airflow_cont \
  airflow users create \
  --role Admin \
  --username $AIRFLOW_USER --password $AIRFLOW_PASSWORD \
  -e $AIRFLOW_USER_EMAIL \
  -f airflow \
  -l airflow && \
  echo "Created airflow Initial admin user with username $AIRFLOW_USER"
