import zmq

def main():
    context = zmq.Context()
    receiver = context.socket(zmq.PULL)
    receiver.connect("tcp://localhost:5555")

    try:
        while True:
            try:
                message = receiver.recv_string()
                print(f"Received message: {message}")
            except zmq.ZMQError as e:
                print(f"Failed to receive message: {e}")
    except KeyboardInterrupt:
        print("Shutting down the puller...")
    finally:
        receiver.close()
        context.term()

if __name__ == "__main__":
    main()


