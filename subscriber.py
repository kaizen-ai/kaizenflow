import pika

# Callback function to process incoming messages
def callback(ch, method, properties, body):
    print(f" first - Received new message: {body}")

# Establish connection to RabbitMQ server
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declare a fanout exchange named 'logs'
channel.exchange_declare(exchange='logs', exchange_type='fanout')

# Declare a unique queue (exclusive=True) with RabbitMQ assigned name
result = channel.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue

# Bind the queue to the 'logs' exchange
channel.queue_bind(exchange='logs', queue=queue_name)

print(' [*] Waiting for logs. To exit press CTRL+C')

# Start consuming messages from the queue
channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
channel.start_consuming()
