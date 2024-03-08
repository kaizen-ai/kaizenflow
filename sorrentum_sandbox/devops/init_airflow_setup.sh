#!/bin/sh

# WARNING: Run this script only during initial Airflow DB setup.

AIRFLOW_USER=airflow
AIRFLOW_PASSWORD=airflow
AIRFLOW_USER_EMAIL=airflow@airflow.com

WEBSERVER_CONTAINER_NAME="airflow_cont"
SCHEDULER_CONTAINER_NAME="airflow_scheduler_cont"

echo "# Initializing Airflow DB setup in webserver"
docker exec \
    -ti \
    $WEBSERVER_CONTAINER_NAME \
    airflow db migrate && \
    echo "Initialized airflow DB"

echo "# Initializing Airflow DB setup in scheduler"
docker exec \
    -ti \
    $SCHEDULER_CONTAINER_NAME \
    airflow db migrate && \
    echo "Initialized airflow DB"

echo "# Setting up Admin user with username $AIRFLOW_USER"
docker exec \
    -ti \
    $WEBSERVER_CONTAINER_NAME \
    airflow users create \
      --role Admin \
      --username $AIRFLOW_USER --password $AIRFLOW_PASSWORD \
      -e $AIRFLOW_USER_EMAIL \
      -f airflow \
      -l airflow
