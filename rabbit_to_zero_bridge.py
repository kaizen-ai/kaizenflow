import pika
import zmq
import sys

def main():
    # Set up ZeroMQ publisher
    context = zmq.Context()
    zmq_socket = context.socket(zmq.PUB)
    zmq_socket.bind("tcp://*:5558")

    # Set up RabbitMQ consumer
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='hello')

    def callback(ch, method, properties, body):
        print(f"Received from RabbitMQ: {body.decode()}")
        zmq_socket.send_string(body.decode())  # Send message via ZeroMQ
        print(f"Sent to ZeroMQ: {body.decode()}")

    channel.basic_consume(queue='hello', on_message_callback=callback, auto_ack=True)
    
    try:
        print('Bridge is running. To exit press CTRL+C')
        channel.start_consuming()
    except KeyboardInterrupt:
        print('Exiting...')
    finally:
        channel.stop_consuming()
        connection.close()
        zmq_socket.close()
        context.term()

if __name__ == "__main__":
    main()
