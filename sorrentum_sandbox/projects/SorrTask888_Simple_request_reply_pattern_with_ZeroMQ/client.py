import zmq
import time

def client():
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect("tcp://server:5555")
    
    for request_num in range(10):
        # Send request to server
        print("Sending request:", request_num)
        socket.send_multipart([b"",b"Client request"])
        
        # Receive response from server
        message = socket.recv_multipart()
        address, response = message
        print("Received response:", response.decode())

if __name__ == "__main__":
    client()

