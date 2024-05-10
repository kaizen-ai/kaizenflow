# import pika

# # Callback function to process incoming messages
# def callback(ch, method, properties, body):
#     print(f" first - Received new message: {body}")

# # Establish connection to RabbitMQ server
# connection = pika.BlockingConnection(pika.ConnectionParameters(host = 'localhost'))
# channel = connection.channel()

# # Declare a fanout exchange named 'logs'
# channel.exchange_declare(exchange='logs', exchange_type='fanout')

# # Declare a unique queue (exclusive=True) with RabbitMQ assigned name
# result = channel.queue_declare(queue='', exclusive=True)
# queue_name = result.method.queue

# # Bind the queue to the 'logs' exchange
# channel.queue_bind(exchange='logs', queue=queue_name)

# print(' [*] Waiting for logs. To exit press CTRL+C')

# # Start consuming messages from the queue
# channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
# channel.start_consuming()





# -------------------------------------------------------------------------------------
# -------------------------------------------------------------------------------------
# -------------------------------------------------------------------------------------
# -------------------------------------------------------------------------------------


# import pika

# def callback(ch, method, properties, body):
#     print(f"Subscriber {properties.app_id} received message: {body}")
#     ch.basic_ack(delivery_tag=method.delivery_tag)  # Acknowledge message

# connection_parameters = pika.ConnectionParameters(host='localhost')

# # Establish connection to RabbitMQ server
# connection = pika.BlockingConnection(connection_parameters)
# channel = connection.channel()

# # Declare a durable fanout exchange named 'logs'
# channel.exchange_declare(exchange='logs', exchange_type='fanout', durable=True)

# # Declare a unique queue with a unique binding key for each subscriber
# queue_name = f"log_queue_{hash(str(id(connection)))}"  # Unique queue name
# channel.queue_declare(queue=queue_name, durable=True)
# channel.queue_bind(exchange='logs', queue=queue_name)

# print(f' [*] Waiting for logs in queue {queue_name}. To exit press CTRL+C')

# # Start consuming messages from the queue
# channel.basic_consume(queue=queue_name, on_message_callback=callback)
# channel.start_consuming()

# -------------------------------------------------------------------------------------
# -------------------------------------------------------------------------------------
# -------------------------------------------------------------------------------------
# -------------------------------------------------------------------------------------


# import pika

# def callback(ch, method, properties, body):
#     print(f"Subscriber {properties.app_id} received message: {body}")
#     # Acknowledge message
#     ch.basic_ack(delivery_tag=method.delivery_tag)

#     # Publish acknowledgment to the acknowledgment exchange
#     ch.basic_publish(
#         exchange='ack_exchange',
#         routing_key='',
#         body=body,
#         properties=pika.BasicProperties(
#             delivery_mode=2,  # Make message persistent
#             correlation_id=properties.correlation_id  # Use correlation ID to identify the acknowledgment
#         )
#     )

# def main():
#     connection_parameters = pika.ConnectionParameters(
#         host='my-rabbit',
#         credentials=pika.PlainCredentials('user', 'user')  # username and password as 'user'
#     )
    
#     connection = pika.BlockingConnection(connection_parameters)
#     channel = connection.channel()

#     channel.exchange_declare(exchange='logs', exchange_type='fanout', durable=True)

#     # Declare a unique queue with a unique binding key for each subscriber
#     queue_name = f"log_queue_{hash(str(id(connection)))}"  # Unique queue name
#     channel.queue_declare(queue=queue_name, durable=True)
#     channel.queue_bind(exchange='logs', queue=queue_name)

#     # Declare a queue for receiving acknowledgments from subscribers
#     ack_queue_name = "ack_queue"
#     channel.queue_declare(queue=ack_queue_name)
#     channel.queue_bind(exchange='ack_exchange', queue=ack_queue_name)

#     # Set up a direct exchange for acknowledgment propagation
#     channel.exchange_declare(exchange='ack_exchange', exchange_type='direct')
#     channel.queue_bind(exchange='ack_exchange', queue=ack_queue_name)

#     print(f' [*] Waiting for logs in queue {queue_name}. To exit press CTRL+C')

#     # Start consuming messages from the queue
#     channel.basic_consume(queue=queue_name, on_message_callback=callback)
#     channel.start_consuming()

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

# def callback(ch, method, properties, body):
#     print(f"Subscriber {properties.app_id} received message: {body}")
#     ch.basic_ack(delivery_tag=method.delivery_tag)
#     ch.basic_publish(
#         exchange='ack_exchange',
#         routing_key='',
#         body=body,
#         properties=pika.BasicProperties(
#             delivery_mode=2,
#             correlation_id=properties.correlation_id
#         )
#     )

# def setup_subscriber(channel):
#     # This might also need to declare the exchanges if not already done elsewhere
#     channel.exchange_declare(exchange='logs', exchange_type='fanout', durable=True)
#     channel.exchange_declare(exchange='ack_exchange', exchange_type='direct', durable=True)

#     # Declare a unique queue for this subscriber to receive logs
#     queue_name = f"log_queue_{hash(str(id(channel)))}"
#     channel.queue_declare(queue=queue_name, durable=True)
#     channel.queue_bind(exchange='logs', queue=queue_name)

#     # Setup for receiving and sending acknowledgments
#     ack_queue_name = "ack_queue"
#     channel.queue_declare(queue=ack_queue_name, durable=True)
#     channel.queue_bind(exchange='ack_exchange', queue=ack_queue_name)

#     return queue_name, ack_queue_name


# def main():
#     connection_parameters = pika.ConnectionParameters(
#         host='my-rabbit',
#         credentials=pika.PlainCredentials('user', 'user')
#     )

#     connection = connect_to_rabbitmq(connection_parameters)
#     channel = connection.channel()

#     ack_queue_name = setup_subscriber(channel)

#     channel.exchange_declare(exchange='logs', exchange_type='fanout', durable=True)

#     queue_name = f"log_queue_{hash(str(id(connection)))}"
#     channel.queue_declare(queue=queue_name, durable=True)
#     channel.queue_bind(exchange='logs', queue=queue_name)

#     ack_queue_name = "ack_queue"
#     channel.queue_declare(queue=ack_queue_name, durable=True)
#     channel.queue_bind(exchange='ack_exchange', queue=ack_queue_name)

#     channel.exchange_declare(exchange='ack_exchange', exchange_type='direct')
#     channel.queue_bind(exchange='ack_exchange', queue=ack_queue_name)

#     print(f' [*] Waiting for logs in queue {queue_name}. To exit press CTRL+C')
#     channel.basic_consume(queue=queue_name, on_message_callback=callback)
#     channel.start_consuming()

# if __name__ == '__main__':
#     main()


# -------------------------------------------------------------------------------------
# -------------------------------------------------------------------------------------
# -------------------------------------------------------------------------------------
# -------------------------------------------------------------------------------------


# import pika
# import time

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

# def callback(ch, method, properties, body):
#     print(f"Subscriber received message: {body}")
#     # Manually send the acknowledgement after processing the message
#     ch.basic_ack(delivery_tag=method.delivery_tag)

# def main():
#     connection_parameters = pika.ConnectionParameters(
#         host='my-rabbit',
#         credentials=pika.PlainCredentials('user', 'user')
#     )

#     connection = connect_to_rabbitmq(connection_parameters)
#     channel = connection.channel()

#     # Declare the fanout exchange for logs
#     channel.exchange_declare(exchange='logs', exchange_type='fanout', durable=True)


#     # Declare a queue with RabbitMQ assigned name, without exclusive flag
#     result = channel.queue_declare(queue='', durable=True)

#     queue_name = result.method.queue

#     # Bind the queue to the 'logs' exchange
#     channel.queue_bind(exchange='logs', queue=queue_name)

#     print(f' [*] Waiting for logs in queue {queue_name}. To exit press CTRL+C')
#     # Set auto_ack to False to manually control the acknowledgement
#     channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)
#     channel.start_consuming()

# if __name__ == '__main__':
#     main()


# import pika
# import time

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

# def callback(ch, method, properties, body):
#     print(f"Subscriber received message: {body}")
#     # Introduce a delay to simulate processing time
#     time.sleep(10)  # Delay for 10 seconds before acknowledging
#     ch.basic_ack(delivery_tag=method.delivery_tag)

# def main():
#     connection_parameters = pika.ConnectionParameters(
#         host='my-rabbit',
#         credentials=pika.PlainCredentials('user', 'user')
#     )

#     connection = connect_to_rabbitmq(connection_parameters)
#     channel = connection.channel()

#     # Declare the fanout exchange for logs
#     channel.exchange_declare(exchange='logs', exchange_type='fanout', durable=True)

#     # Declare a queue with RabbitMQ assigned name, without exclusive flag
#     result = channel.queue_declare(queue='', durable=True)
#     queue_name = result.method.queue

#     # Bind the queue to the 'logs' exchange
#     channel.queue_bind(exchange='logs', queue=queue_name)

#     print(f' [*] Waiting for logs in queue {queue_name}. To exit press CTRL+C')
#     channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)
#     channel.start_consuming()

# if __name__ == '__main__':
#     main()
# ------------------------------------
import pika
import time

def connect_to_rabbitmq(parameters, retry_delay=10, max_retries=10):
    for attempt in range(max_retries):
        try:
            return pika.BlockingConnection(parameters)
        except pika.exceptions.AMQPConnectionError as error:
            print(f"Attempt {attempt + 1}: Connection failed, error: {error}")
            if attempt < max_retries - 1:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print("Failed to connect to RabbitMQ after several attempts.")
                raise

def callback(ch, method, properties, body):
    print(f"Subscriber received message: {body}")
    # Manually send the acknowledgement after processing the message
    time.sleep(5)  # delay for testing visibility
    ch.basic_ack(delivery_tag=method.delivery_tag)

def main():
    connection_parameters = pika.ConnectionParameters(
        host='my-rabbit',
        credentials=pika.PlainCredentials('user', 'user')
    )

    connection = connect_to_rabbitmq(connection_parameters)
    channel = connection.channel()

    # Declare the fanout exchange for logs
    channel.exchange_declare(exchange='logs', exchange_type='fanout', durable=True)

    # Declare a queue with RabbitMQ assigned name, without exclusive flag
    result = channel.queue_declare(queue='', durable=True)
    queue_name = result.method.queue

    # Bind the queue to the 'logs' exchange
    channel.queue_bind(exchange='logs', queue=queue_name)

    # Set prefetch count
    channel.basic_qos(prefetch_count=1)

    print(f' [*] Waiting for logs in queue {queue_name}. To exit press CTRL+C')
    channel.basic_consume(queue=queue_name, on_message_callback=callback)
    channel.start_consuming()

if __name__ == '__main__':
    main()


