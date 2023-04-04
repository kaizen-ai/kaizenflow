#!/bin/bash -e
docker run -itd -p 80:80 --name kaiko kaiko:1.0 airflow webserver