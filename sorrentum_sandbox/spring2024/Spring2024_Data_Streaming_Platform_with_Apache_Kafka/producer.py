import json
import random
import time
import logging
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer, KafkaException

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class Env:
    def __init__(self, conf):
        self.conf = conf
        self.producer = Producer(conf)
        self.admin_client = AdminClient(conf)


def check_kafka_topic_created(env, topic):
    admin_client = env.admin_client
    cluster_metadata = admin_client.list_topics()
    return topic in cluster_metadata.topics.keys()


def create_kafka_topic(env, topic_name):
    admin_client = env.admin_client
    topic = NewTopic(topic=topic_name, num_partitions=3, replication_factor=1)

    if check_kafka_topic_created(env, topic_name):
        logging.warning(f"Topic {topic_name} already exists")
    else:
        logging.info(f"Creating topic {topic_name}")
        admin_client.create_topics([topic])[topic_name].result()

        if check_kafka_topic_created(env, topic_name):
            logging.info(f"Topic {topic_name} created")
        else:
            logging.error(f"Topic {topic_name} not created")
            raise Exception(f"Topic {topic_name} not created")


def acked(err, msg):
    if err is not None:
        logging.error(f"Failed to deliver message: {err}")
    else:
        logging.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def generate_trade():
    return {
        "e": "trade",
        "E": int(time.time() * 1000),
        "s": random.choice(["BTCUSDT", "BNBUSDT", "BNBBTC"]),
        "t": random.randint(1, 100000),
        "p": "{:.6f}".format(random.uniform(0.0001, 0.01)),
        "q": str(random.randint(1, 10000)),
        "b": random.randint(1, 500),
        "a": random.randint(501, 1000),
        "T": int(time.time() * 1000),
        "m": random.choice([True, False]),
        "M": True,
    }


def send_trade(env, topic):
    producer = env.producer
    try:
        trade = generate_trade()
        logging.info("Sending trade data to Kafka: %s", trade)
        producer.produce(topic, value=json.dumps(trade), callback=acked)
        producer.poll(0)
    except KafkaException as e:
        logging.error(f"An error occurred: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
    finally:
        producer.flush()


if __name__ == "__main__":

    conf = {"bootstrap.servers": "broker:9092", "queue.buffering.max.messages": 1000000}
    env = Env(conf)
    topic_name = "trades"

    create_kafka_topic(env, topic_name)

    while True:
        send_trade(env, topic_name)
        time.sleep(random.randrange(0, 3))
