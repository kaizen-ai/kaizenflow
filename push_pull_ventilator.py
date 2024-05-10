import zmq
import sys

def main():
    context = zmq.Context()
    socket = context.socket(zmq.PUSH)
    socket.bind("tcp://*:5558")

    print("Enter tasks for workers:")
    try:
        while True:
            task = input("Enter task (Ctrl + C to quit): ")
            socket.send_string(task)
            print(f"Task sent: {task}")

    finally:
        socket.close()
        context.term()

if __name__ == "__main__":
    main()
