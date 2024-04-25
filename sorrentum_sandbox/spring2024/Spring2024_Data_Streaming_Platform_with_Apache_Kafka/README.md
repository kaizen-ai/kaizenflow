# Data Streaming Platform with Apache Kafka

## Overview

## Understanding Kafka

## Project Structure
- `docker-compose.yml`
- `Dockerfile`
- `requirements.txt`
- `.gitignore`
- `kafka_tutorial.ipynb`
- `producer.py`
- `consumer.py`

## How to Run
- Run the following command to build, start, and run all of our Docker containers defined in our `docker-compose.yml` file in detached mode (running in background)
```sh
> docker compose up -d
 ```
-  Run the following command to list the containers defined in our `docker-compose.yml` file, showing their current state, ports, and other details.
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

In this section, we will go through interactive code examples in Jupyter Notebook where we:
1. Create topics and define partitions and replication factors.
2. Create producers and send messages to the topic.
3. Create consumers that read and process messages from the topic.
4. Set up a PostgreSQL database and create a table for storing messages.
5. Create consumers that read and process messages from the topic, and insert them into our database.
6. Explore data in our PostgreSQL database.

To get started, make sure all the containers are up and running, then
1. Open your web browser and go to [http://localhost:8888](http://localhost:8888) to access the Jupyter Notebook.
2. Locate and open the `kafka_tutorial.ipynb` notebook.
3. Follow along by reading and executing each cell in the notebook.

## Data Streaming Platform

In the section, we will build a simple data streaming platform where a producer continuously generates messages and sends them to a topic in our Apache Kafka cluster. A consumer will subscribe to this topic, read and process the messages in real time, and then insert the processed data into our PostgreSQL database.

- To start a producer, open another terminal window and run the following commands:
```sh
❯ docker exec -it jupyter bash
root@8453789a0344:/app# python producer.py
2024-04-25 19:29:26,603 - INFO - Creating topic trades
2024-04-25 19:29:26,641 - INFO - Topic trades created
2024-04-25 19:29:26,641 - INFO - Sending trade data to Kafka: {'e': 'trade', 'E': 1714073366641, 's': 'BTCUSDT', 't': 19863, 'p': '0.008072', 'q': '5362', 'b': 66, 'a': 604, 'T': 1714073366641, 'm': True, 'M': True}
2024-04-25 19:29:27,606 - INFO - Message delivered to trades [2]
2024-04-25 19:29:27,606 - INFO - Sending trade data to Kafka: {'e': 'trade', 'E': 1714073367606, 's': 'BNBUSDT', 't': 47487, 'p': '0.005291', 'q': '3767', 'b': 69, 'a': 979, 'T': 1714073367606, 'm': True, 'M': True}
2024-04-25 19:29:27,609 - INFO - Message delivered to trades [2]
...
```

-  To start a consumer, open another terminal window and run the following commands:
```sh
❯ docker exec -it jupyter bash
root@8453789a0344:/app# python consumer.py
Database 'trades' created successfully.
Table 'binance' created successfully
Starting the consumer loop.
Received message: {'e': 'trade', 'E': 1714073366641, 's': 'BTCUSDT', 't': 19863, 'p': '0.008072', 'q': '5362', 'b': 66, 'a': 604, 'T': 1714073366641, 'm': True, 'M': True} from partition 2
Record inserted
Received message: {'e': 'trade', 'E': 1714073367606, 's': 'BNBUSDT', 't': 47487, 'p': '0.005291', 'q': '3767', 'b': 69, 'a': 979, 'T': 1714073367606, 'm': True, 'M': True} from partition 2
Record inserted
....
```

- To check records that have been inserted into our table, open another terminal window and run the following command:
```sh
❯ docker exec -it jupyter bash
root@8453789a0344:/app# export PGPASSWORD='postgres';
root@8453789a0344:/app# psql -U postgres -h pgdatabase -d trades -c "SELECT COUNT(1) FROM binance;"
 count
-------
   126
(1 row)

root@8453789a0344:/app# psql -U postgres -h pgdatabase -d trades -c "SELECT COUNT(1) FROM binance;"
 count
-------
   142
(1 row)
```

## Cleaning Up
- Run the following command to stop and removes all containers, networks, and volumes as well as any containers connected to the network defined in our `docker-compose.yml` file
```sh
docker compose down -v --remove-orphans
```

## Conclusion