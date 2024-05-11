# UDP CHAT APPLICATION

## Description
Implement a simple chat application using UDP for communication. Create a client-server model where multiple clients can connect to a server using UDP sockets. Messages sent by one client should be broadcast to all connected clients. Explore error handling and reliability in UDP connections.

## Overview
The UDP Chat Application is a real-time messaging system built using the User Datagram Protocol (UDP), a connectionless protocol that prioritizes speed over reliability. This application allows multiple clients to connect to a central server and exchange messages with each other in a chat room-like environment.

### Architecture
The application follows a client-server architecture, with the server acting as a central hub for broadcasting messages to all connected clients. The server and clients communicate using UDP sockets, which enable fast and efficient data transfer without the overhead of establishing and maintaining dedicated connections.

## Technology Used
Here's the information presented in a single paragraph or line for each bullet point:

**Python**:
The application is written in Python, a versatile and widely-used high-level, interpreted programming language known for its simplicity, readability, vast ecosystem of libraries, and suitability for developing network applications and handling concurrent operations.

**Socket Library**:
The `socket` module in Python, which provides low-level networking interfaces for creating and managing sockets, including UDP sockets used in this application for enabling communication between the server and clients.

**Threading Library**:
The `threading` module in Python, which allows for creating and managing threads, enabling the server to handle multiple client connections simultaneously and the clients to receive messages while sending new ones through multithreading.

**Queue Library**:
The `queue` module in Python, which provides a synchronized queue class for handling thread-safe data structures, used in this application to temporarily store incoming messages from clients before broadcasting them to all other connected clients.

**Docker**:
Docker, a platform for building, deploying, and running applications in isolated environments called containers, used in this application to containerize both the server and client components, ensuring portability and ease of deployment.

**Docker Compose**:
Docker Compose, a tool for defining and running multi-container Docker applications, used in this application to define and manage the server and client containers as a single application, simplifying the build and run process.

**Dockerfile**:
Dockerfiles, text files containing instructions for building Docker images, used in this application to define the instructions for building the Docker images for the server and client components separately.

## Understanding Network Protocol


### Transmission Control Protocol

TCP (Transmission Control Protocol) is one of the main protocols of the Internet protocol suite. It lies between the Application and Network Layers which are used in providing reliable delivery services. It is a connection-oriented protocol for communications that helps in the exchange of messages between different devices over a network. The Internet Protocol (IP), which establishes the technique for sending data packets between computers, works with TCP. 

### Features of TCP
- TCP keeps track of the segments being transmitted or received by assigning numbers to every single one of them.
- Flow control limits the rate at which a sender transfers data. This is done to ensure reliable delivery.
- TCP implements an error control mechanism for reliable data transfer.
- TCP takes into account the level of congestion in the network.

![TCP](https://media.geeksforgeeks.org/wp-content/uploads/20230406111816/TCP-1.png)


### User Datagram Protocol (UDP)
User Datagram Protocol (UDP) is a Transport Layer protocol. UDP is a part of the Internet Protocol suite, referred to as the UDP/IP suite. Unlike TCP, it is an unreliable and connectionless protocol. So, there is no need to establish a connection before data transfer. The UDP helps to establish low-latency and loss-tolerating connections establish over the network. The UDP enables process-to-process communication.

### Features of UDP
- Used for simple request-response communication when the size of data is less and hence there is lesser concern about flow and error control.
- It is a suitable protocol for multicasting as UDP supports packet switching.
- UDP is used for some routing update protocols like RIP(Routing Information Protocol).
- Normally used for real-time applications which can not tolerate uneven delays between sections of a received message.

![UDP](https://media.geeksforgeeks.org/wp-content/uploads/20230406112517/TCP-2.png)

#### Which Protocol is Better: TCP or UDP?
The answer to this question is difficult because it totally depends on what work we are doing and what type of data is being delivered. UDP is better in the case of online gaming as it allows us to work lag-free. TCP is better if we are transferring data like photos, videos, etc. because it ensures that data must be correct has to be sent.

In general, both TCP and UDP are useful in the context of the work assigned by us. Both have advantages upon the works we are performing, that’s why it is difficult to say, which one is better.

## Understanding  Pyhton script

### Server

The Server python script creates a simple UDP-based chat server that allows clients to send and receive messages in a chatroom-like environment. Here's a breakdown of how the script works:

1. **Importing Libraries:**
   - The script imports the necessary libraries: `socket` for socket programming, `threading` for creating multiple threads, and `queue` for handling message queues.

2. **Initialization:**
   - It initializes two data structures: a queue named `messages` to store incoming messages from clients, and a list named `clients` to store the addresses of connected clients.

3. **Socket Creation and Binding:**
   - It creates a UDP socket named `server` using the `socket.socket()` function. The socket is bound to the localhost address (`"localhost"`) and port number `9999` using the `bind()` method.

4. **Receive Function:**
   - The `receive()` function runs in a loop and listens for incoming messages from clients.
   - When a message is received, it is added to the `messages` queue along with the address of the sender (`addr`).

5. **Broadcast Function:**
   - The `broadcast()` function also runs in a loop.
   - It continuously checks the `messages` queue for new messages. If a message is present, it dequeues the message and sender address.
   - It then broadcasts the message to all connected clients by iterating over the `clients` list and sending the message to each client's address.
   - If a new client joins (indicated by a message starting with "SIGNUP_TAG:"),
     - It extracts the client's name from the message and sends a notification to all clients that the new client has joined.
     - Otherwise, it simply broadcasts the message.

6. **Thread Creation and Start:**
   - Two threads (`t1` and `t2`) are created to run the `receive()` and `broadcast()` functions concurrently.
   - The threads are started using the `start()` method.

Overall, this script enables clients to send messages to the server, which are then broadcasted to all connected clients. It handles message reception and broadcasting concurrently using two separate threads to ensure smooth operation of the chat server.

### Client

The Client python script creates a simple UDP-based chat client that allows users to send and receive messages in a chatroom-like environment. Below is an explanation of how the script works:

1. **Importing Libraries:**
   - The script imports the `socket` library for socket programming and the `threading` library for creating multiple threads.

2. **Socket Creation and Binding:**
   - It creates a UDP socket named `client` using the `socket.socket()` function. The socket is bound to the localhost address (`"localhost"`) and any available port (`0`) using the `bind()` method.

3. **User Input:**
   - The script prompts the user to enter their nickname (`name`) using the `input()` function.

4. **Receive Function:**
   - The `receive()` function runs in a loop and listens for incoming messages from the server.
   - When a message is received, it prints the decoded message to the console.

5. **Thread Creation and Start:**
   - A single thread (`t`) is created to run the `receive()` function concurrently.
   - The thread is started using the `start()` method.

6. **Sending Signup Message:**
   - The client sends a signup message to the server to announce its nickname using the `sendto()` method. The message starts with "SIGNUP_TAG:" followed by the user's nickname.
   - The message is encoded using `encode()` and sent to the server's address (`"localhost", 9999`).

7. **Sending and Handling User Messages:**
   - The client enters a loop to continuously send messages to the server.
   - It prompts the user to input a message using the `input()` function.
   - If the user inputs "!q", the client closes the socket and exits the program.
   - Otherwise, the client sends the message, including the user's nickname, to the server using the `sendto()` method.
   - If an error occurs during message sending, the client closes the socket and exits the program.

This script enables users to interact with the chat server by sending and receiving messages. It runs a separate thread to handle incoming messages asynchronously, ensuring that the client can both send and receive messages simultaneously.

## Docker Initialization

### Dockerfile Server
This Dockerfile sets up a Docker image for running a Python server script. Here's how it works:

1. **Base Image:**
   - It starts with the official Python image (`python:3.9-slim`) as the base image. This image provides a minimal Python environment based on Python version 3.9.

2. **Working Directory:**
   - It sets the working directory inside the container to `/app` using the `WORKDIR` instruction. This directory will be used to store the server script and serve as the context for running commands.

3. **Copying Server Script:**
   - It copies the server script (`server.py`) from the host machine to the `/app` directory inside the container using the `COPY` instruction. This ensures that the script is available inside the container for execution.

4. **Exposing Port:**
   - It exposes port `9999` using the `EXPOSE` instruction. This indicates that the container will listen for incoming connections on port `9999`, allowing external communication with the server running inside the container.

5. **Command to Run Server:**
   - It specifies the command to run the server script using the `CMD` instruction. In this case, it runs the Python interpreter (`python`) with the `server.py` script as an argument. This command will be executed when the container starts.

### Dockerfile Client
This Dockerfile is designed to create a Docker image for running a Python client script. Here's a breakdown of its components:

1. **Base Image:**
   - It utilizes the official Python image (`python:3.9-slim`) as the base image. This provides a lightweight Python environment based on Python version 3.9.

2. **Working Directory:**
   - The `WORKDIR` instruction sets the working directory inside the container to `/app`. This directory will serve as the location for storing the client script and executing commands.

3. **Copying Client Script:**
   - The `COPY` instruction copies the client script (`client.py`) from the host machine to the `/app` directory within the container. This ensures that the script is available inside the container for execution.

4. **Command to Run Client:**
   - The `CMD` instruction specifies the command to execute the client script. In this case, it runs the Python interpreter (`python`) with `client.py` as an argument. This command will be executed when the container is launched.

## Docker-Compose

The Docker Compose file is used to define and manage a multi-container Docker application consisting of a UDP chat server and a UDP chat client. Here's a breakdown of its contents:

1. **Version Specification:**
   - The `version` field specifies the version of Docker Compose file syntax being used. In this case, it's version `3.5`.

2. **Services:**
   - Under the `services` section, different components of the application are defined.

3. **UDP Chat Server Service:**
   - The `udp-chat-server` service is defined first.
   - **Build Configuration:**
     - It specifies to build the service using the Dockerfile located at `./server/Dockerfile`.
   - **Port Mapping:**
     - Port `9999` on the host is mapped to port `9999` on the container using the `ports` directive. The protocol used is UDP.
   - **Network Configuration:**
     - It sets the `network_mode` to `host`, allowing the container to use the host network stack. This is useful for UDP communication as it simplifies network configuration.
   
4. **UDP Chat Client Service:**
   - The `udp-chat-client` service is defined next.
   - **Build Configuration:**
     - It specifies to build the service using the Dockerfile located at `./client/Dockerfile`.
   - **Network Configuration:**
     - Similar to the server service, it sets the `network_mode` to `host` to utilize the host network stack.
   - **Interactive Mode:**
     - `stdin_open` and `tty` are set to `true` to allow interactive mode, enabling the client to accept user input from the terminal.

This Docker Compose file simplifies the deployment and management of the UDP chat server and client components by defining their configurations and dependencies in a single YAML file. It allows for easy scaling and reproducibility of the application environment.


## Container Building and Working of Docker-Compose


- **Building Images**

```
docker compose build
```

- **Running Containers**

Running `docker-compose run --rm udp-chat-server` will create and run a new container based on the configuration defined for the udp-chat-server service in the docker-compose.yml file.

Here's what each part of the command does:

`docker-compose run`: This command is used to run a one-time command in a service container. It creates a new container for the specified service, executes the command, and then removes the container after the command finishes executing.

`--rm`: This flag tells Docker Compose to remove the container after it exits. This helps keep your system clean by automatically removing temporary containers created for the command.

`udp-chat-server`: This is the name of the service defined in your docker-compose.yml file. Docker Compose will look for the configuration of this service in the YAML file and create a container based on that configuration.

This command will open a server container which will broadcast the chats between clients:

**First instance**

```
➜  Final Project docker compose run --rm udp-chat-server


```

**Same for client we will run `docker-compose run --rm udp-chat-client` in two terminals.**

It will show a prompt of entering "NICKNAME:" for userID.

```
➜  Final Project docker-compose run --rm udp-chat-client
WARN[0000] /Users/abhinav/Documents/Final Project/docker-compose.yml: `version` is obsolete 
NICKNAME:   
```

After entering prompts in two clients container, as Jack and Jill.

Our server container will look like this:

```
SIGNUP_TAG:jill
SIGNUP_TAG:jack
```


**Now we will display instances of chats exchanges between Jack and Jill**

- Jill

```
NICKNAME:jill
 jill joined!
jack joined!
hey jack
 jill: hey jack
jack: hi jill
i liked the data 605 classes a lot
 jill: i liked the data 605 classes a lot
jack: yeah me too
```

- Jack

```
NICKNAME:jack
 jack joined!
jill: hey jack
hi jill
 jack: hi jill
jill: i liked the data 605 classes a lot
yeah me too
 jack: yeah me too
```

- Server container which broadcast all the chats.

```
SIGNUP_TAG:jill
SIGNUP_TAG:jack
jill: hey jack
jack: hi jill
jill: i liked the data 605 classes a lot
jack: yeah me too
```

- For exiting the container we will use keyboard shortcut ` cntrl + c` as we have an interactive shell.

### Cleaning Up

Run the following command to stop and removes all containers, networks, and volumes as well as any containers connected to the network defined in our `docker-compose.yml` file

```
docker compose down -v --remove-orphans
```

## Conclusion

Certainly! Here's a suggestion for the conclusion section of your README file:

---

## Conclusion

In this project, we successfully developed a simple UDP-based chat application using Python and Docker. The application consists of a server component responsible for handling message broadcasting and client registration, as well as a client component allowing users to connect to the chat server, send messages, and receive messages from other users.

### Key Features

- **UDP Protocol**: The chat application utilizes the User Datagram Protocol (UDP) for communication between the server and clients, providing a lightweight and connectionless communication mechanism.
- **Server-Client Architecture**: The application follows a server-client architecture, where the server manages message broadcasting and client registration, while clients interact with the server to send and receive messages.
- **Dockerization**: We Dockerized the application using Docker and Docker Compose, allowing for easy deployment and management of the server and client components as containerized services.

### Usage

To run the chat application, follow these steps:

1. Make sure you have Docker installed on your system.
2. Clone this repository to your local machine.
3. Navigate to the directory containing the Dockerfiles and `docker-compose.yml` file.
4. Run the following command to build and start the containers:

```bash
docker-compose up --build
```

5. Once the containers are running, open multiple terminal windows or tabs to simulate multiple users joining the chat.
6. Interact with the chat application by sending and receiving messages using the client terminals.
7. When done, gracefully stop the containers by pressing `Ctrl + C` in the terminal where you ran the `docker-compose up` command, or use the following command:

```bash
docker-compose down
```

### Future Enhancements

While the current implementation provides a basic chat functionality, there are several opportunities for enhancement:

- **Message Persistence**: Implement message persistence to allow users to view previous messages upon joining the chat.
- **User Authentication**: Introduce user authentication mechanisms to ensure secure access to the chat application.
- **Message Encryption**: Implement message encryption to enhance the security and privacy of communication between users.
- **Graphical User Interface (GUI)**: Develop a graphical user interface for the chat application to improve usability and user experience.

### References

- https://www.geeksforgeeks.org/how-to-create-a-multi-chat-server-using-udp/
- https://www.geeksforgeeks.org/differences-between-tcp-and-udp/
- ChatGPT

## Video Demonstration

https://drive.google.com/file/d/1iNjM7hD4e1ToxvB2ZszB1kgrKCzXzl9R/view?usp=sharing

