# Data Streaming Platform with Apache Kafka

## Overview

## Understanding Kafka

## Project Structure
- `docker-compose.yml`
- `Dockerfile`
- `requirements.txt`
- `.gitignore`
- `kafka_tutorial.ipynb`

## How to Run
- Run the following command to build (if necessary), create, start, and run all of our Docker containers defined in our docker-compose.yml file in detached mode (running in background)
```sh
> docker compose up -d
 ```
-  Run the following command to list the containers defined in our docker-compose.yml file, showing their current state, ports, and other details.
```sh
❯ docker compose ps
NAME              IMAGE                                                          COMMAND                  SERVICE           CREATED         STATUS         PORTS
broker            confluentinc/cp-kafka:7.2.0                                    "/etc/confluent/dock…"   broker            3 minutes ago   Up 7 seconds   0.0.0.0:9092->9092/tcp
jupyter           spring2024_data_streaming_platform_with_apache_kafka-jupyter   "jupyter notebook --…"   jupyter           3 minutes ago   Up 7 seconds   0.0.0.0:8888->8888/tcp
pgdatabase        postgres:14.0                                                  "docker-entrypoint.s…"   pgdatabase        3 minutes ago   Up 7 seconds   0.0.0.0:5432->5432/tcp
schema-registry   confluentinc/cp-schema-registry:7.2.0                          "/etc/confluent/dock…"   schema-registry   3 minutes ago   Up 7 seconds   0.0.0.0:8081->8081/tcp
zookeeper         confluentinc/cp-zookeeper:7.2.0                                "/etc/confluent/dock…"   zookeeper         3 minutes ago   Up 7 seconds   2888/tcp, 0.0.0.0:2181->2181/tcp, 3888/tcp
```

## Kafka Tutorial
Once all containers are up and running:
1. Open your web browser and go to [http://localhost:8888](http://localhost:8888) to access the Jupyter Notebook.
2. Locate and open the `kafka_tutorial.ipynb` notebook.
3. Follow along by reading and executing each cell in the notebook.

## End to End Data Streaming Platform

## Cleaning Up
- Run the following command to stop and removes all containers, networks, and volumes as well as any containers connected to the network defined in our `docker-compose.yml` file
```sh
docker compose down -v --remove-orphans
```

## Conclusion