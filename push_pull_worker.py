import zmq

def main():
    context = zmq.Context()
    socket = context.socket(zmq.PULL)
    socket.connect("tcp://localhost:5558")

    print("Worker ready to receive tasks...")
    while True:
        task = socket.recv_string()
        print(f"Received task: {task} - Processing")

if __name__ == "__main__":
    main()