import zmq
import time

def main():
    context = zmq.Context()
    sender = context.socket(zmq.PUSH)
    sender.bind("tcp://*:5555")

    try:
        count = 0
        while True:
            message = f"Message {count}: Data from the pusher"
            try:
                sender.send_string(message)
                print(f"Sent: {message}")
            except zmq.ZMQError as e:
                print(f"Failed to send message: {e}")
            time.sleep(1)
            count += 1
    except KeyboardInterrupt:
        print("Shutting down the pusher...")
    finally:
        sender.close()
        context.term()

if __name__ == "__main__":
    main()
