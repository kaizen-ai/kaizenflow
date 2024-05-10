# Basic Pubsub

# import pika
# from pika.exchange_type import ExchangeType

# connection_parameters = pika.ConnectionParameters(host='localhost')

# # test comment
# # Establish connection to RabbitMQ server
# connection = pika.BlockingConnection(connection_parameters)
# channel = connection.channel()

# # Declare a fanout exchange named 'logs'
# channel.exchange_declare(exchange='logs', exchange_type='fanout')

# # Prompt user to enter log messages to be published
# while True:
#     message = input("Enter log message (press 'q' to quit): ")
#     if message.lower() == 'q':
#         break
    
#     # Publish the message to the 'logs' exchange with an empty routing key (fanout exchange)
#     channel.basic_publish(exchange='logs', routing_key='', body=message)
#     print(f" [x] Sent: {message}")

# # Close the connection
# connection.close()


# -------------------------------------------------------------------------------------
# -------------------------------------------------------------------------------------
# -------------------------------------------------------------------------------------
# -------------------------------------------------------------------------------------

# # second pub - message is being send to all subscriber
# import pika

# connection_parameters = pika.ConnectionParameters(host='localhost')

# # Establish connection to RabbitMQ server
# connection = pika.BlockingConnection(connection_parameters)
# channel = connection.channel()

# # Declare a durable fanout exchange named 'logs'
# channel.exchange_declare(exchange='logs', exchange_type='fanout', durable=True)

# # Prompt user to enter log messages to be published
# while True:
#     message = input("Enter log message (press 'q' to quit): ")
#     if message.lower() == 'q':
#         break
    
#     # Publish the message to the 'logs' exchange with an empty routing key (fanout exchange)
#     channel.basic_publish(
#         exchange='logs', 
#         routing_key='', 
#         body=message,
#         properties=pika.BasicProperties(
#             delivery_mode=2,  # Make message persistent
#         )
#     )
#     print(f" [x] Sent: {message}")

# # Close the connection
# connection.close()



# -------------------------------------------------------------------------------------
# -------------------------------------------------------------------------------------
# -------------------------------------------------------------------------------------
# -------------------------------------------------------------------------------------


# import pika

# def main():
#     connection_parameters = pika.ConnectionParameters(
#         host='my-rabbit',
#         credentials=pika.PlainCredentials('user', 'user')  # username and password as 'user'
#     )
    
#     connection = pika.BlockingConnection(connection_parameters)
#     channel = connection.channel()

#     channel.exchange_declare(exchange='logs', exchange_type='fanout', durable=True)

#     # Declare a queue for receiving acknowledgments from subscribers
#     ack_queue_name = "ack_queue"
#     channel.queue_declare(queue=ack_queue_name)
#     channel.queue_bind(exchange='ack_exchange', queue=ack_queue_name)

#     # Set up a direct exchange for acknowledgment propagation
#     channel.exchange_declare(exchange='ack_exchange', exchange_type='direct')

#     while True:
#         message = input("Enter log message (press 'q' to quit): ")
#         if message.lower() == 'q':
#             break

#         # Publish the message to the 'logs' exchange with an empty routing key (fanout exchange)
#         channel.basic_publish(
#             exchange='logs',
#             routing_key='',
#             body=message,
#             properties=pika.BasicProperties(
#                 delivery_mode=2,  # Make message persistent
#                 reply_to=ack_queue_name  # Specify acknowledgment queue
#             )
#         )
#         print(f" [x] Sent: {message}")

#         # Wait for acknowledgment from the acknowledgment exchange
#         print(" [x] Waiting for acknowledgment from the subscriber...")
#         method_frame, header_frame, body = channel.basic_get(queue=ack_queue_name, auto_ack=True)
#         if method_frame:
#             print(" [x] Message acknowledged by subscriber")
#         else:
#             print(" [!] No acknowledgment received from subscriber")

#     connection.close()

# if __name__ == '__main__':
#     main()


# -------------------------------------------------------------------------------------
# -------------------------------------------------------------------------------------
# -------------------------------------------------------------------------------------
# -------------------------------------------------------------------------------------



# import pika
# import time

# def connect_to_rabbitmq(parameters, retry_delay=10, max_retries=10):  # Increased delay and retries
#     for attempt in range(max_retries):
#         try:
#             return pika.BlockingConnection(parameters)
#         except pika.exceptions.AMQPConnectionError as error:
#             print(f"Attempt {attempt + 1}: Connection failed, error: {error}")
#             if attempt < max_retries - 1:
#                 print(f"Retrying in {retry_delay} seconds...")
#                 time.sleep(retry_delay)
#             else:
#                 print("Failed to connect to RabbitMQ after several attempts.")
#                 raise

# def setup_publisher(channel):
#     # Declare the fanout exchange for logs
#     channel.exchange_declare(exchange='logs', exchange_type='fanout', durable=True)

#     # Declare the direct exchange and queue for acknowledgments
#     channel.exchange_declare(exchange='ack_exchange', exchange_type='direct', durable=True)
#     ack_queue_name = "ack_queue"
#     channel.queue_declare(queue=ack_queue_name, durable=True)
#     channel.queue_bind(exchange='ack_exchange', queue=ack_queue_name)

#     return ack_queue_name

# def main():
#     connection_parameters = pika.ConnectionParameters(
#         host='my-rabbit',
#         credentials=pika.PlainCredentials('user', 'user')
#     )

#     connection = connect_to_rabbitmq(connection_parameters)
#     channel = connection.channel()

#     ack_queue_name = setup_publisher(channel)
#     channel.exchange_declare(exchange='logs', exchange_type='fanout', durable=True)

#     ack_queue_name = "ack_queue"
#     channel.queue_declare(queue=ack_queue_name, durable=True)
#     channel.queue_bind(exchange='ack_exchange', queue=ack_queue_name)

#     channel.exchange_declare(exchange='ack_exchange', exchange_type='direct', durable = True)

#     while True:
#         message = input("Enter log message (press 'q' to quit): ")
#         if message.lower() == 'q':
#             break

#         channel.basic_publish(
#             exchange='logs',
#             routing_key='',
#             body=message,
#             properties=pika.BasicProperties(
#                 delivery_mode=2,
#                 reply_to=ack_queue_name
#             )
#         )
#         print(f" [x] Sent: {message}")

#         print(" [x] Waiting for acknowledgment from the subscriber...")
#         method_frame, header_frame, body = channel.basic_get(queue=ack_queue_name, auto_ack=True)
#         if method_frame:
#             print(" [x] Message acknowledged by subscriber")
#         else:
#             print(" [!] No acknowledgment received from subscriber")

#     connection.close()

# if __name__ == '__main__':
#     main()

# -------------------------------------------------------------------------------------
# -------------------------------------------------------------------------------------
# -------------------------------------------------------------------------------------
# -------------------------------------------------------------------------------------


# import pika
# import time
# import random

# def connect_to_rabbitmq(parameters, retry_delay=10, max_retries=10):
#     for attempt in range(max_retries):
#         try:
#             return pika.BlockingConnection(parameters)
#         except pika.exceptions.AMQPConnectionError as error:
#             print(f"Attempt {attempt + 1}: Connection failed, error: {error}")
#             if attempt < max_retries - 1:
#                 print(f"Retrying in {retry_delay} seconds...")
#                 time.sleep(retry_delay)
#             else:
#                 print("Failed to connect to RabbitMQ after several attempts.")
#                 raise

# def setup_publisher(channel):
#     # Declare the fanout exchange for logs
#     channel.exchange_declare(exchange='logs', exchange_type='fanout', durable=True)

#     # Declare the direct exchange and queue for acknowledgments
#     channel.exchange_declare(exchange='ack_exchange', exchange_type='direct', durable=True)
#     ack_queue_name = "ack_queue"
#     channel.queue_declare(queue=ack_queue_name, durable=True)
#     channel.queue_bind(exchange='ack_exchange', queue=ack_queue_name)

#     return ack_queue_name

# def generate_log_message():
#     log_levels = ["INFO", "DEBUG", "WARNING", "ERROR", "CRITICAL"]
#     message = f"Log Message - Level: {random.choice(log_levels)}"
#     return message

# def main():
#     connection_parameters = pika.ConnectionParameters(
#         host='my-rabbit',
#         credentials=pika.PlainCredentials('user', 'user')
#     )

#     connection = connect_to_rabbitmq(connection_parameters)
#     channel = connection.channel()

#     ack_queue_name = setup_publisher(channel)

#     try:
#         while True:
#             message = generate_log_message()
#             channel.basic_publish(
#                 exchange='logs',
#                 routing_key='',
#                 body=message,
#                 properties=pika.BasicProperties(
#                     delivery_mode=2,
#                     reply_to=ack_queue_name
#                 )
#             )
#             print(f" [x] Sent: {message}")
#             time.sleep(1)  # Pause for a second before sending the next message

#     except KeyboardInterrupt:
#         print("Publisher stopped by user.")

#     finally:
#         connection.close()

# if __name__ == '__main__':
#     main()


# -------------------------------------------------------------------------------------
# -------------------------------------------------------------------------------------
# -------------------------------------------------------------------------------------
# -------------------------------------------------------------------------------------

import pika

def setup_exchange_and_queue():
    connection_parameters = pika.ConnectionParameters(
        host='my-rabbit',
        credentials=pika.PlainCredentials('user', 'user')
    )
    connection = pika.BlockingConnection(connection_parameters)
    channel = connection.channel()

    # Declare the fanout exchange for logs
    channel.exchange_declare(exchange='logs', exchange_type='fanout', durable=True)

    # Declare the direct exchange and queue for acknowledgments
    channel.exchange_declare(exchange='ack_exchange', exchange_type='direct', durable=True)
    ack_queue_name = "ack_queue"
    channel.queue_declare(queue=ack_queue_name, durable=True)
    channel.queue_bind(exchange='ack_exchange', queue=ack_queue_name)

    connection.close()

if __name__ == '__main__':
    setup_exchange_and_queue()
