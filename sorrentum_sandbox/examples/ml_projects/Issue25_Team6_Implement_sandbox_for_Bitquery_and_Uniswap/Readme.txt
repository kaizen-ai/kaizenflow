
Instructions for creating the dockerfile:
docker build -t data605 .
docker run -p 8888:8888 -p 5432:5432 -p 8080:8080 --name data605_container -d data605

Airflow login:
Username: airflow
Password: airflow

Postgres airflow username and password:
Username: docker
Password: docker
database: db