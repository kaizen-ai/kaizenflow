import pika
from pika.exchange_type import ExchangeType

connection_parameters = pika.ConnectionParameters(host='localhost')

# test comment
# Establish connection to RabbitMQ server
connection = pika.BlockingConnection(connection_parameters)
channel = connection.channel()

# Declare a fanout exchange named 'logs'
channel.exchange_declare(exchange='logs', exchange_type='fanout')

# Prompt user to enter log messages to be published
while True:
    message = input("Enter log message (press 'q' to quit): ")
    if message.lower() == 'q':
        break
    
    # Publish the message to the 'logs' exchange with an empty routing key (fanout exchange)
    channel.basic_publish(exchange='logs', routing_key='', body=message)
    print(f" [x] Sent: {message}")

# Close the connection
connection.close()
