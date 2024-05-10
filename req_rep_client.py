import zmq

def main():
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect("tcp://localhost:5557")
    
    try:
        while True:
            message_send = input("Enter message to send (Ctrl + C to exit): ")
            
            socket.send_string(message_send)
            message_received = socket.recv_string()
            print(f"Received reply: {message_received}")
    finally:
        socket.close()
        context.term()

if __name__ == "__main__":
    main()