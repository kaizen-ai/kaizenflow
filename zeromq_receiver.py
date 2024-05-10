import zmq

def main():
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect("tcp://localhost:5558")
    #socket.bind("tcp://*:5557")
    socket.setsockopt_string(zmq.SUBSCRIBE, "")

    print("Waiting for messages from ZeroMQ...")
    while True:
        message = socket.recv_string()
        print(f"Received: {message}")
  
if __name__ == "__main__":
    main()
