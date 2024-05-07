import socket
import threading

client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
client.bind(("localhost", 0))  # Bind to any available port

name = input("NICKNAME:")

def receive():
    while True:
        try:
            message, _ = client.recvfrom(4096)  # Increased buffer size
            print(message.decode())
        except socket.error as e:
            print(f"Error receiving message: {e}")

t = threading.Thread(target=receive)
t.start()

client.sendto(f"SIGNUP_TAG:{name}".encode(), ("localhost", 9999))

while True:
    message = input(" ")
    if message == "!q":
        client.close()  # Close the socket before exiting
        exit()
    else:
        try:
            client.sendto(f"{name}: {message}".encode(), ("localhost", 9999))
        except socket.error as e:
            print(f"Error sending message: {e}")
            client.close()
            exit()