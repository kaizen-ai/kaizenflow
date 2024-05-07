# UDP CHAT APPLICATION

## Description
Implement a simple chat application using UDP for communication. Create a client-server model where multiple clients can connect to a server using UDP sockets. Messages sent by one client should be broadcast to all connected clients. Explore error handling and reliability in UDP connections.

## Overview
The UDP Chat Application is a real-time messaging system built using the User Datagram Protocol (UDP), a connectionless protocol that prioritizes speed over reliability. This application allows multiple clients to connect to a central server and exchange messages with each other in a chat room-like environment.

### Architecture
The application follows a client-server architecture, with the server acting as a central hub for broadcasting messages to all connected clients. The server and clients communicate using UDP sockets, which enable fast and efficient data transfer without the overhead of establishing and maintaining dedicated connections.

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