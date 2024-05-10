# Apache Kafka to create a data streaming platform

## Author info

- Author: Gabriel Oladipupo Lawson
- GitHub account: DIPOLAWSON
- UMD email: OLAWSON1@UMD.EDU
- Personal email: LAWSONOLADIPUPO200@GMAIL.COM

## Description

The project involves setting up Apache Kafka and a PostgreSQL database encapsulated in a single or multiple Docker containers with docker-compose to establish a streamlined data streaming platform. Utilize Python to fetch data from an external source, format it for Kafka ingestion (Topics), and configure producers for efficient data transfer into Kafka topics. Python-based Kafka consumers will perform some EDA using Jupyter notebook, process and validate the data before storing it into PostgreSQL, utilizing a predefined schema. The goal is to create a reliable system that seamlessly downloads, processes, and securely stores external data in real-time using Kafka as the intermediary, Python for logic handling, and Docker for deployment flexibility.

## Technologies

## Zookeeper
As a centralized service, Zookeeper handles configuration data management, naming, distributed synchronization, and group service support. 
Distributed systems depend on the preservation of a "source of truth" regarding the setup and state of various services inside the network. 
In environments where several services necessitate consistent and reliable coordination, Zookeeper makes sure that all involved nodes are in sync with one another, preventing discrepancies in service statuses and configurations.

## Key Configurations
#### ZOOKEEPER_CLIENT_PORT
The port on which Zookeeper waits for client connections is indicated by this variable. 
It is necessary to enable communication between Zookeeper and client apps, such as Kafka.

#### ZOOKEEPER_TICK_TIME
The basic time unit in Zookeeper is the duration of a single tick, which is expressed in milliseconds. 
Processes like heartbeat and timeouts, which are essential for preserving the functionality and overall health of the Zookeeper service, are impacted by this parameter.

## Kafka
### Kafka's Setup Linked with Zookeeper
Apache Kafka uses Zookeeper to maintain cluster configurations and states. 
To ensure fault tolerance and data consistency throughout the cluster, Kafka brokers use Zookeeper to manage leader election of partitions and keep track of which brokers are active.
### Key Configurations
#### KAFKA_BROKER_ID
A cluster's unique identification for every Kafka broker.
The ability of the Kafka system to recognize and control various brokers is essential, particularly for scaling up or down. 
#### KAFKA_ZOOKEEPER_CONNECT
Indicates the hostname and port of the Zookeeper service as part of the connection string for Zookeeper. 
Since Zookeeper is used by Kafka to maintain its internal state, this connection is essential to its proper operation. 
#### KAFKA_ADVERTISED_LISTENERS
Sets up the address that producers and customers will see the broker promote. 
This configuration is essential for client connections as well as network communication within the Kafka cluster. 
#### KAFKA_LISTENER_SECURITY_PROTOCOL_MAP
Specifies the protocols that listeners utilize. 
This information is crucial for setting up security and communication protocol standards. 

## PostgreSQL
### Usage of PostgreSQL
A relational database called PostgreSQL is used to efficiently store and handle project data. 
It is the best option for managing massive amounts of data with high transaction rates since it provides strong data integrity and support for sophisticated queries. 
### Configuration Details 
#### Ports
Applications from outside can connect to the database by mapping external port 5320 to internal PostgreSQL port 5432. 
#### Volumes
When data persistence is used with volumes, information is preserved through container restarts and removals. 
#### Configuration
Configuring the default user, password, and database name in the environment's POSTGRES_USER, POSTGRES_PASSWORD, and POSTGRES_DB settings guarantees that the application has the right authorization to safely communicate with the database. 



## Python-App
### Configuration for Multiple Modes
The Python application container may switch between a conventional web application and a Jupyter Notebook environment thanks to its special configuration.
Because of this flexibility, developers, and analysts can run the application in a production-like environment or build and analyze in a Jupyter Notebook, depending on what suits their needs at the moment.
#### APP_MODE
This environment variable is very important since it controls whether the container runs the web application or launches a Jupyter server. 
The container's usefulness is increased in this configuration, meeting requirements for both development and operations.
#### Dependencies
The Python program depends on PostgreSQL for data storage and Kafka for data streaming.
This guarantees that every component of the data pipeline is linked and ready to go before the application launches. 

## Docker: Containerization
Docker is a comprehensive platform that makes leveraging containerization technologies to create, ship, and execute applications easier.
With the help of containers, software developers can bundle a program together with all of its dependencies into a standardized unit that will guarantee consistent operation across all environments.
Docker's functionality is expanded with the help of Docker Compose, a tool for creating and managing multi-container Docker applications. With Docker Compose, you can use a YAML configuration file to define the services, networks, and volumes, and you can use a single command to configure, run, and stop all the components of a complicated application.
Docker is a containerization platform that allows developers to package applications and their dependencies into lightweight, portable containers.
-  Docker containers provide a high level of isolation, allowing applications to run independently of the underlying host system.
-  Docker simplifies dependency management by allowing developers to specify the exact environment needed for their applications. This reduces compatibility issues and ensures that applications run consistently across different development machines and servers.
-   Docker encourages a modular approach to application development, where applications are broken down into smaller, independent services running in separate containers. This facilitates the adoption of microservices architecture, allowing for easier maintenance, updates, and scaling of individual components.












