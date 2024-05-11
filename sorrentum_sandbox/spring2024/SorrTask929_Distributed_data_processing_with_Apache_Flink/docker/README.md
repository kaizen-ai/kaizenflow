# Distributed Data Processing using Apache Flink
Running Apache Flink containers using Docker Compose is a convenient way to get up and running to try out some Flink workloads.

Assuming docker compose is installed you can start the containers using the following command in the same folder as the docker compose file:

```
docker compose up -d
```

This will start the containers in the background and you can check that the containers are running using

To start adding some work to Flink you can access the Flink console using the following command and from there you can try out various jobs like creating tables.

```bash
docker-compose exec jobmanager ./bin/flink run -py /opt/docker/flink_job.py -d
```