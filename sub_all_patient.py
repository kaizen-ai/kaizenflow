#example with receiving all the messages. Could be useful for an administrator or record keeping.

import zmq

def create_subscriber():
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect("tcp://localhost:5555")  # Connect to the publisher socket
    socket.setsockopt_string(zmq.SUBSCRIBE, '')  # Subscribe to all messages

    while True:
        message = socket.recv_string() 
        print(message)

create_subscriber()
