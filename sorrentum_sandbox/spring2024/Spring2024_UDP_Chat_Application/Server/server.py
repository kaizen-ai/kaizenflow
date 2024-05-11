import socket
import threading
import queue

messages = queue.Queue()
clients = []

server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
server.bind(("localhost", 9999))

def receive():
    while True:
        try:
            message, addr = server.recvfrom(4096)  # Increased buffer size
            messages.put((message, addr))
        except socket.error as e:
            print(f"Error receiving message: {e}")

def broadcast():
    while True:
        while not messages.empty():
            message, addr = messages.get()
            print(message.decode(), flush=True)
            if addr not in clients:
                clients.append(addr)
            for client in clients[:]:  # Iterate over a copy of the list for safe removal
                try:
                    if message.decode().startswith("SIGNUP_TAG:"):
                        name = message.decode()[len("SIGNUP_TAG:"):]
                        server.sendto(f"{name} joined!".encode(), client)
                    else:
                        server.sendto(message, client)
                except socket.error as e:
                    print(f"Error sending message to {client}: {e}")
                    clients.remove(client)  # Remove client if sending fails

t1 = threading.Thread(target=receive)
t2 = threading.Thread(target=broadcast)

t1.start()
t2.start()