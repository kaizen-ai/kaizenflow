import zmq

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:5557")

while True:
    message = socket.recv_string()
    print(f"Received request: {message}")
    socket.send_string("World")