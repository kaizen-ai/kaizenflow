# Final Project Report

## Technology Used
ZeroMQ is a powerful messaging library that streamlines communication between applications by providing
various messaging patterns, including the push/pull model central to this project. One of the key 
features of ZeroMQ is its lack of a central message broker, which distinguishes it from other messaging
technologies such as MQTT or AMQP. In ZeroMQ, messages are sent directly from one application to 
another, eliminating the need for a middleman. This approach reduces latency, simplifies the system
architecture, and removes common bottlenecks associated with brokers.

Without a broker, ZeroMQ offers faster communication and simpler deployment, but this setup also requires
that any additional features typically handled by a broker, such as message queuing and load balancing, 
must be managed by the application itself. This can add complexity to the system design when such 
features are necessary. Furthermore, ensuring messages are delivered reliably is also up to the 
developer, as ZeroMQ does not automatically handle these tasks.

Despite these challenges, ZeroMQ's flexibility and adaptability across various platforms make it an 
excellent choice for developing distributed applications that need to handle multiple data streams 
simultaneously. It allows developers to concentrate on building the functionality of their 
applications without getting bogged down by the underlying network communication details. ZeroMQ's 
ability to facilitate efficient and scalable communication is particularly beneficial in environments
where high performance and low overhead are crucial.

Overall, ZeroMQ is well-suited for projects that require robust communication solutions and can handle
the additional complexity of managing some network functions traditionally done by brokers. It's
performance advantages often outweigh the initial complexity, making it a valuable tool for modern 
software development.

### Docker System
The project utilizes Docker to create a consistent and isolated environment for the ZeroMQ messaging 
system, by ensuring that the application is portable and behaves the same across different setups. Below, 
the structure of the Docker setup will be explained, detailing the roles of the containers, the Dockerfile configuration, and the communication setup between the containers.

- **Base Image**: Starting from the `python:3.8-slim` image. This version of the Python image provides all necessary Python functionalities without unnecessary extras, keeping the container small.
- **Working Directory**: The `WORKDIR` instruction is set to `/app`. This instruction specifies that all subsequent actions should be taken from the `/app` directory inside the container, which helps in keeping the application files organized.
- **Add Files**: The `ADD . /app` command copies all files from the host's current directory to `/app` in the container. This includes the Python scripts and any other files needed for the application.
- **Install Dependencies**: `RUN pip install --no-cache-dir -r requirements.txt` installs the Python dependencies defined in `requirements.txt`. The `--no-cache-dir` option is used to reduce the build size by not storing the extra cache data.
- **Default Command**: The `CMD ["python", "pusher.py"]` command sets the container to run `pusher.py` by default when it starts. This script initiates the message sending process.

- **Services**:
  - **Pusher**: Configures the container that runs the `pusher.py` script. It builds the container using the Dockerfile, maps the current directory to `/app` inside the container, and exposes port 5555 for ZeroMQ to send messages.
  - **Puller**: Similar to the pusher, but the container runs `puller.py` by default. This script listens for incoming messages. It uses the same build context and volume mapping as the pusher.

- The pusher and puller containers communicate using TCP, facilitated by the exposed and mapped port 
5555. The pusher sends messages over this port, which the puller listens to. Docker's networking allows 
these containers to communicate as though they are on the same local network, simplifying configuration 
and enhancing performance.

This Docker setup supports ZeroMQ's broker-less architecture effectively while ensuring simplicity in 
deployment and scalability. Using a lightweight Python base image and organizing files and commands 
neatly within the Dockerfile and Docker Compose enhances the system's efficiency and manageability. This
setup can be easily scaled or adapted to different environments as required.

#### Running the System
Running the ZeroMQ messaging system using Docker involves a series of simple steps, especially by leveraging the Docker Compose tool to simplify the process. This section will describe the commands to start the container system as well as provide an example of how the output looks when the system is operational.

To start the system, navigate to the directory containing the docker-compose.yml file. This file contains all the configuration needed to build and run the containers for both the pusher and the puller processes. You can proceed by:

1. Opening a Terminal: Start by opening a command-line interface (CLI) such as Terminal on macOS, which is what I used

2. Navigating to the Project Directory: Use the cd command to change the directory to where your docker-compose.yml is located. I used:
cd /Users/waverlydiggs/DATA_605/sorrentum

3. Run Docker Compose: Execute the following command to build the Docker images and start the containers:
docker-compose up --build

This command tells Docker to compile the images defined in the Dockerfile and then start both the pusher and puller containers.

4. Observe the Output: Once the containers are running, Docker Compose will stream logs to the terminal. These logs show the output 
from both the pusher and puller scripts. You should see messages like:

pusher_1  | Sent: Message 0: Data from the pusher
puller_1  | Received message: Message 0: Data from the pusher
pusher_1  | Sent: Message 1: Data from the pusher
puller_1  | Received message: Message 1: Data from the pusher

5. Stop the System: To stop the containers, you can press Ctrl+C in the terminal where Docker Compose is running

##### What Was Done

The primary components are two Python scripts, pusher.py and puller.py, operating within a structured Docker 
environment to establish a robust messaging system using ZeroMQ. This setup leverages ZeroMQ’s efficient push/pull messaging pattern,
providing a detailed insight into its practical application and demonstrating how Docker can be utilized to ensure an isolated and 
consistent setup across different environments.

The pusher.py script functions as the message sender in this system. It creates a ZeroMQ context and establishes a PUSH socket, 
binding it to a specified port where it can send out messages to any listening counterpart. The script enters a loop where it 
constructs and sends messages continuously. Each message includes a sequence number, enhancing traceability and debugging by clearly indicating the message order. The script prints each message to the console before sending, which provides real-time logging of its activity and is essential for monitoring the system’s operation in a production or testing environment. This is what happens in the pusher.py script:

def main():
    context = zmq.Context()  # Creates a new ZeroMQ context
    sender = context.socket(zmq.PUSH)  # Creates a PUSH socket
    sender.bind("tcp://*:5555")  # Binds the socket to listen on all interfaces at port 5555

    try:
        count = 0
        while True:
            message = f"Message {count}: Data from the pusher"  # Constructs a message with a count
            print(f"Sent: {message}")  # Logs the message to the console
            sender.send_string(message)  # Sends the message over the socket
            time.sleep(1)  # Pauses for one second to control the rate of message sending
            count += 1  # Increments the message count for the next message
    except KeyboardInterrupt:
        print("Shutting down the pusher...")  # Handles a keyboard interrupt to gracefully shut down
    finally:
        sender.close()  # Closes the socket
        context.term()  # Terminates the context

if __name__ == "__main__":
    main()

Similarly, the puller.py script acts as the message receiver. It sets up a ZeroMQ context and a PULL socket, then connects to the pusher’s socket. The script continuously waits to receive messages, printing each received message to the console. This not only confirms successful communication between the pusher and puller but also allows real-time monitoring of the messages being processed. Below is the breakdown of the puller.py script:

def main():
    context = zmq.Context()  # Establishes a new ZeroMQ context for socket operations
    receiver = context.socket(zmq.PULL)  # Creates a PULL socket to receive messages
    receiver.connect("tcp://localhost:5555")  # Connects to the pusher’s socket via localhost on port 5555

    try:
        while True:
            message = receiver.recv_string()  # Blocks and waits to receive a message from the socket
            print(f"Received message: {message}")  # Prints the received message to the console
    except KeyboardInterrupt:
        print("Shutting down the puller...")  # Provides a handler for graceful shutdown on keyboard interrupt
    finally:
        receiver.close()  # Closes the socket
        context.term()  # Terminates the ZeroMQ context

if __name__ == "__main__":
    main()

When the system runs, the terminal displays a stream of logs showing the messages being sent and received. This live output is crucial for understanding the flow of data and ensuring that all components are functioning as expected. An example of the typical output when both scripts are active is:

pusher_1  | Sent: Message 0: Data from the pusher
puller_1  | Received message: Message 0: Data from the pusher
pusher_1  | Sent: Message 1: Data from the pusher
puller_1  | Received message: Message 1: Data from the pusher

This output confirms that the pusher is effectively sending messages which are then being received by the puller. It also helps in diagnosing any potential issues in real-time, such as delays in message delivery or failures in message receipt, which are critical for troubleshooting and ensuring the reliability of the communication system.