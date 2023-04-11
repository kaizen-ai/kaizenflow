#!/bin/bash -xe

# Start PostgreSQL
service postgresql start

# # Initialize Airflow database
# airflow db initdb

# Start Airflow webserver
airflow webserver &

# Start Jupyter notebook
jupyter-notebook --port=8888 --no-browser --ip=0.0.0.0 --allow-root --NotebookApp.token='' --NotebookApp.password=''

