import zmq

def server():
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://*:5555")
    
    while True:
        # Receive request from client
        message = socket.recv_multipart()
        
        address, req = message
        print("Received request:", req.decode())

        # Send response to client
        socket.send_multipart([address, b"Server response"])

if __name__ == "__main__":
    server()

