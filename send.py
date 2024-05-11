import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='hello')

#channel.basic_publish(exchange='', routing_key='hello', body='Hello World!')
#print(" [x] Sent 'Hello World!'")
#connection.close()
try:
    while True:
        message = input("Enter message to publish: ")
        if message.lower() == 'exit':
            print('Exiting...')
            break
        channel.basic_publish(exchange = 'logs', routing_key = '', body = message)
        print(f"Sending message: {message}")
except KeyboardInterrupt:
    print("Exiting...")
finally:
    connection.close()