from confluent_kafka import Producer
import json
import time

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def generate_messages(producer, topic):
    i = 0
    while True:
        message = {
            "client": i % 2 + 1,
            "amount": 100,
            "ts": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        producer.produce(topic, json.dumps(message), callback=delivery_report)
        producer.flush()
        time.sleep(5)  # Produce a message every 5 seconds
        i += 1

def main():
    conf = {'bootstrap.servers': 'kafka:9092'}
    producer = Producer(**conf)
    topic = 'client_amount'
    generate_messages(producer, topic)

if __name__ == '__main__':
    main()
