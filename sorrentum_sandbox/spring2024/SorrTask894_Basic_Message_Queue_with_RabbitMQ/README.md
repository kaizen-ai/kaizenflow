# DATA605_Message_Queue
DATA605 Project

**What is RabbitMQ**

Welcome to Kaizenflow. Here, we created a basic message queue using the technology RabbitMQ. A message queue is a basic fundamental concept in computer programming and software 
development. For this one, we used RabbitMQ, which is an open source message broker that was developed by Pivotal Software, that is used for message queue technology. It utilizes AMQP, 
short for Advanced Message Queuing Protocol, for communication between services. It is written in Erlang, and it is built on the Open Telecom Platform framework for clustering and 
fallover. Client libraries to interface with this broker are all usable for major programming languages. As a message queue, it serves to manage the routing, queuing, and delivery of 
messages from senders (publishers/producers) to receivers (consumers). It has a versatile publish-subscribe model, where messages are sent as "exchanges", routing them to relevant 
queues depending on the configurable rules. It uses a variety of messaging patterns, such as point-to-point, fan-out, and topic-based, and request-response, making it incredibly 
adaptable to diverse needs. The main kinds of problems RabbitMQ as a technology intends to solve are quite a few. The first is its ability to decouple applications, allowing more 
scalable and reliable applications that can communicate with one another without being a monolith. It also helps to build 12-factor architectures, designing applications as a loosely 
coupled services in a collection with one another. It can also improve the performance of applications by implementing asynchronous communication, sending and receiving messages without 
blocking the sender or the receiver in doing so. It also performs a variety of other tasks with messages, such as real-time streaming (live chat), load balancing of messages, providing 
fallover in case message sending fails, as well as auditing (tracking) who sent messages.

One of the main similar technologies that RabbitMQ competes with is Apache Kafka. However, unlike Kafka, RabbitMQ routes messages based on a rule, as opposed to Kafka's use of 
publishing and subscribing to a topic from publishers and consumers. The result is that Kafka message receivers who are subscribed to a given partition will receive any messages sent to 
that partition no matter what, but routed messages from RabbitMQ can be filtered by consumers to specify which messages they are specifically interested in receiving and which messages 
they are not interested in receiving. RabbitMQ also delivers messages in an ordered sequence, which isn't the case with Kafka due to data partitioning. However, with RabbitMQ, there is 
no guarantee about what order these messages are sent to a queue or exchange. It will send in a set order so long as there is only one message consumer, but it may be less consistent if 
we have multiple message receivers. Because Kafka involves data partitioning, it may process messages sent to the same partition in a desired order, which it does by default using a 
round robin partitioner. However, the producer also has input in how Kafka messages are partitioned. RabbitMQ is push-based, pushing messages to its consumers, whereas Kafka is a pull-
based system that requires the consumers to always be pulling for new messages from the server. RabbitMQ also evicts messages once they are consumed, whereas Kafka may preserve them for 
a determined length of time until that retention period expires. RabbitMQ is, however, effective at having tools to handle failures of a system such as dead-letter exchanges and 
delivery retries to prevent message failures, whereas with Kafka, it is incumbent upon the producer to create their own failure management systems.

A notable aspect of RabbitMQ compared to other technologies is its adherence to AMPQ, which allows it to be interoperable and compatible with a multitude of programming languages. This
makes it very seamless to integrate and scale, which makes it very suitable for Big Data processing.

The pros of using RabbitMQ as a message queue are quite a few. The first is that it is a very durable and reliable form of message queue. RabbitMQ is a technology that ensures the 
messages are persisted to the disk, providing a reliable form of message storage and delivery for producers. RabbitMQ's routing system and rules also make for a very flexible system, 
one that is very dynamic in nature ane enables multiple messages to be sent to multiple queues, and that each message is sent to the correct device or service that it needs to be sent 
to without fail. RabbitMQ is also relatively scalable, as it can handle a large number of messages and a vast amount of connections between servers, making it very effective in high-
traffic applications. Thankfully, RabbitMQ is also very popular to the point where it has extensive community support, making it very available to learn through tutorials and other 
widely available resources. On the other hand, RabbitMQ has a very steep learning curve, due to its complex configuration and language (Erlang is by no means an intuitive writing to 
understand), and has more limited throughput compared to other messaging systems, such as Kafka. This contrasts its main competitor, Kafka, which has its own benefits such as decoupling 
and extensive integration with other technologies, and has higher throughput. In spite of these, RabbitMQ does have its advantages over its competitor Kafka and as a message queue 
itself and can be quite useful in several applications.

In general, RabbitMQ takes great advantage of a lot of big data technologies and topics. Its fault-tolerance and scalability make it very adaptable to complement a variety of models 
such as SQL and NoSQL, using them to handle high amounts of messages in a high throughput. It is a fundamental component in Apache Airflow pipelines thanks to the above aspects, with 
its integration into Apache pipelines allowing it to perform its task of efficient message queuing and in turn allowing for streamlined data wrangling and deployment of its data in the
form of messages that are sent and received. RabbitMQ also works with the use of modern storage and cloud computing in its production, transmission, and storage of messages. Because it 
sends messages from one database to another, and in many cases will send messages to multiple databases at a time, it uses these to store data in the form of messages created by message
producers from RabbitMQ, and to send them to multiple receivers at once. 

References:

https://betterprogramming.pub/introduction-to-message-queue-with-rabbitmq-python-639e397cb668

https://betterprogramming.pub/rabbitmq-vs-kafka-1779b5b70c41

https://medium.com/@ansam.yousry/choosing-the-right-messaging-system-rabbitmq-vs-kafka-vs-amqp-369d02063785

https://tanzu.vmware.com/content/blog/understanding-the-differences-between-rabbitmq-vs-kafka

https://www.upsolver.com/blog/kafka-versus-rabbitmq-architecture-performance-use-case

**Note on Docker**

While I would ideally have liked to use Docker, it ultimately failed to work in this project. I performed this project using a CloudAMQP server and a Jupyter notebook directly using my
Windows 11 machine. Docker is generally not workable in a Windows environment, and needs to be in a Mac or Linux work environment. For a Windows environment, this would require the use 
of a virtual machine using VMWare, which I attempted to work. However, setting up the virtual machine came with extreme difficulties as the machine would often fail due to lack of space
or crash due to having too much space. In the end, the result was that I performed this project entirely with a Windows machine itself, without the assistance of Docker.

**An overview of the project itself**

![install pika](https://github.com/edmundpark99/DATA605_Message_Queue/assets/6594718/2d9be5ec-acfc-4097-801f-06a1d41e661d)

The very first thing we do in the Python code to start RabbitMQ's basic message queue is to !pip install pika to install the necessary packages, then import the pika package from the 
installled package.

![Basic Message Queue Code 1](https://github.com/edmundpark99/DATA605_Message_Queue/assets/6594718/8d7f9258-eeb3-4f77-9b8e-eee7ae39e90f)

From here, our first lines of code involve connecting directly to the RabbitMQ server.

We establish parameters, which will equal pika.URLParameters and a URL of the connection to the RabbitMQ databse, which in this case is given via CloudAMQP. The URL includes a username
and password, aka credentials, as well as a server URL. The "connection" will be pika.BlockingConnection(parameters) to help to close the connection once it has completed publishing the 
message. The "channel" will equal connection.channel(), which will open the connection upon running.

The credentials and URL are obtained from CloudAMQP, which is shown in this screenshot:

![CloudAMQP Database](https://github.com/edmundpark99/DATA605_Message_Queue/assets/6594718/1116bb24-8bfe-4e96-a697-145c4934e55d)

Above, you can see the CloudAMQP website, which you log into, and it will give you a username and password with which you will copypaste and use in your code above. This will be 
different for every individual.

![Basic Message Queue Code 2](https://github.com/edmundpark99/DATA605_Message_Queue/assets/6594718/1f95edbf-30e8-4011-b2c3-ca284aec9986)

From here, you declare a queue named "Hello!" in the script, and this queue will hold messages sent and received by the script.

![Basic Message Queue Code 3](https://github.com/edmundpark99/DATA605_Message_Queue/assets/6594718/c8dcb547-8e41-4c7d-bcc4-9cf6be4edc71)

The following code is if you intend to publish a message using this code, which it will using the sample message "Hello, RabbitMQ" and send to the queue.

![Basic Message Queue Code 4](https://github.com/edmundpark99/DATA605_Message_Queue/assets/6594718/e5c041c7-1dc1-48eb-a448-9d0266a2a0f2)

The above code is used if you intend to consume/receive a message, using a consumer function "callback" to handle incoming messages. The script will subscribe to the hello queue and 
wait for incoming messages, then the callback function will print the received message, in this case, it's "Hello, RabbitMQ!"

With the above script, it will continue to listen for incoming messages until it is interrupted, such as through pressing CTRL+C.

![Basic Message Queue Code 5](https://github.com/edmundpark99/DATA605_Message_Queue/assets/6594718/bbb15cc5-c894-4b04-b8d6-9d5ada6fad31)

After all of the above code is run and the code is run, the above line will close the connection to the RabbitMQ and AMQP server.

[x] Sent 'Hello, RabbitMQ!' 

The above is a sample output in the event that one runs the code that involves publishing and sending a message.

[*] Waiting for messages. To exit press CTRL+C
 [x] Received b'Hello, RabbitMQ!'

The above is an example of the output of the code in the event that one intends to consume a message.

sequenceDiagram
    participant Script
    participant RabbitMQ
    Script->>RabbitMQ: Connect to RabbitMQ server
    Script->>RabbitMQ: Declare Queue 'hello'
    Script->>RabbitMQ: Publish Message 'Hello, RabbitMQ!'
    RabbitMQ-->>Script: Message Acknowledgement
    Script->>RabbitMQ: Start Consuming from Queue 'hello'
    RabbitMQ-->>Script: Message 'Hello, RabbitMQ!'
    Note over Script: Message Received\n'Hello, RabbitMQ!'

Above is a diagram of how the RabbitMQ basic message queue operates, written in mermaid. It establishes two participants, the "Script" and the "RabbitMQ" server. From there, the 
Script, aka the Python Script we are running, will connect to RabbitMQ, then declare a queue named 'hello'. THe Script will publish a message, and then RabbitMQ acknowledges the 
message, and the script will then start consuming the message from the queue, which the server sends to the script.

In this case, since we are not using Docker due to complications with VMWare, this script does not directly interact with a database. However, if it were to do so by storing messages 
in a database, we might have a sample schema that looks like this:

- Table: "messages"
  - Columns:
      - 'id' (Primary Key, auto-increment)
      - 'content' (Text)
      - 'timestamp' (Datetime)
   
The above would be a basic outline of what a hypothetical database would look like, if one was used.
