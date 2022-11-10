#!/bin/sh

# WARNING: Run this script only during initial airflow db setup.

AIRFLOW_USER=airflow
AIRFLOW_PASSWORD=airflow
AIRFLOW_USER_EMAIL=airflow@airflow.com

echo "Initializing Airflow DB setup and Admin user setup"
echo "Airflow admin username will be $AIRFLOW_USER"
# Execute 
docker exec \
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
