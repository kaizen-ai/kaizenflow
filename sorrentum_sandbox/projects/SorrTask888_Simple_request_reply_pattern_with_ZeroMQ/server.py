import zmq
import time

def server():
    try:
        context = zmq.Context()
        socket = context.socket(zmq.REP)
        socket.bind("tcp://*:5555")
        
        poller = zmq.Poller()
        poller.register(socket, zmq.POLLIN)

        while True:
            socks = dict(poller.poll(timeout=1000))  # Poll for 1 second
            
            if socket in socks and socks[socket] == zmq.POLLIN:
                try:
                    # Receive request from client
                    message = socket.recv_multipart()
                    
                    # Check if the message has the correct format
                    if len(message) != 2:
                        print("Received invalid message from client")
                        socket.send_multipart([address, b"Invalid message"])
                        continue  # Skip processing invalid messages

                    address, req = message
                    print("Received request:", req.decode())

                    # Send response to client
                    socket.send_multipart([address, b"Server response"])
                except zmq.error.ZMQError as e:
                    print(f"ZMQ error: {e}")

            else:
                print("Timeout occurred while waiting for request from client")

    except zmq.error.ZMQError as e:
        print(f"ZMQ error during initialization: {e}")

if __name__ == "__main__":
    server()

