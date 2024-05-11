import zmq
import time

context = zmq.Context()
socket = context.socket(zmq.PUB)
socket.bind("tcp://*:5556")

while True:
    socket.send_string("Test message")
    print("Sent test message")
    time.sleep(1)