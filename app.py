# app.py

import zmq

# Server
def server():
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://*:5555")

    while True:
        message = socket.recv_string()
        print("Received request: %s" % message)
        socket.send_string("Hello from server")

# Client
def client():
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect("tcp://localhost:5555")

    socket.send_string("Hello from client")
    response = socket.recv_string()
    print("Received response: %s" % response)

if __name__ == "__main__":
    # Start the server in a separate thread
    import threading
    server_thread = threading.Thread(target=server)
    server_thread.start()

    # Wait for the server to start
    import time
    time.sleep(1)

    # Run the client
    client()

