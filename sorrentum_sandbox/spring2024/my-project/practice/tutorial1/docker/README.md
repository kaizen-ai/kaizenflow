# Simple Messaging System with RabbitMQ

## Author info

- Author: Youjin Park
- GitHub account: thepeanutbasket (https://github.com/thepeanutbasket)
- UMD email: ypark112@umd.edu
- Personal email: park.youjin331@gmail.com

## Description

This is a Python-based project that leverages RabbitMQ, a robust messaging system, diving deeper into RabbitMQ's capabilities by incorporating message acknowledgement mechanisms to ensure reliable message delivery and fault tolerance. This project takes a step forward from a simple logging system to a more advanced system that uses a topic exchange instead of a direct exchange. With a topic exchange, messages can be routed based on multiple criteria. Specifically, this project will focus on how to subscribe to logs based on the severity of the log as well as the source that generated the log.

## Technologies

### RabbitMQ: Messaging and Streaming Broker
- RabbitMQ is an open-source message broker, which helps applications communicate asynchronously. 
- It acts like a post office: applications send messages (letters) to queues (mailboxes), and RabbitMQ (postal workers) route them based on rules (sorting) to the appropriate queues (recipients) for processing later.
- RabbitMQ decouples applications, improves responsiveness, and allows for easier scaling.
- RabbitMQ is a flexible and scalable messaging system that supports various network protocols and manages the routing and distribution of messages effectively in large-scale systems. 
- RabbitMQ supports several open standard protocols. There are multiple client libraries available, which can be used with many programming languages.
- RabbitMQ provides many options that can be combined to define how messages go from the publisher to one or many consumers: routing, filtering, streaming, federation, and so on.
- With the ability to acknowledge message delivery and to replicate messages across a cluster, messages are safe with RabbitMQ.

### Docker: Portable Container Platform
- Docker is a powerful platform designed to make it easier to create, deploy, and run applications by using containers. 
- Key concepts:
    - Containers: Containers allow a user to package up an application with all of the parts it needs, such as libraries and other dependencies, and ship it all out as one package. They are isolated from each other and the host system.
    - Images: Docker images are lightweight, standalone, executable packages that include everything needed to run a software application: code, runtime, system tools, system libraries, and settings. 
    - Dockerfile: A Docker file is a text document that contains all the commands a user could call on the command line to assemble an image. Using 'docker build', users can create an automated build that executes several command-line instructions in succession.
    - Docker Compose: Docker Compose is a tool for defining an running multi-container Docker applications. With Compose, a use uses a YAML file to configure application's services, networks, and volumes, and then create and start all the services from your configuration with a single command.
- With Docker Containers, users can create predictable environments that are isolated from other applications. Docker ensures that software behaves the same way regardless of where it is deployed.
- Once an application and its dependencies are containerized, the container can be shared among users, and it can run on any system that has Docker installed-regardless of the underlying infrastructure.
- Docker is ideal for building microservice architectures because each part of the application can be independently housed in separate containers. This makes managing each service or part of the app easier and more precise.

## Docker Implementation
