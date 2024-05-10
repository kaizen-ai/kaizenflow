import zmq

context = zmq.Context()
socket = context.socket(zmq.SUB)
socket.connect("tcp://localhost:5556")
socket.setsockopt_string(zmq.SUBSCRIBE, '')

print("Listening for messages...")
while True:
    message = socket.recv_string()
    print(f"Received: {message}")