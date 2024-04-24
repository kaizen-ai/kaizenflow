from elasticsearch import Elasticsearch
from faker import Faker
import json
from datetime import datetime

fake = Faker()

try:
    es = Elasticsearch(
        ['http://localhost:9200'],
        basic_auth=('elastic', 'CpZcVEXMtU04YG515b08')  # Updated to use basic_auth
    )  

    def generate_log_entry():
        entry = {
            "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "ip": fake.ipv4(),
            "method": fake.random_element(elements=("GET", "POST", "DELETE", "PUT")),
            "endpoint": fake.uri_path(),
            "response_code": fake.random_element(elements=(200, 201, 404, 500)),
            "response_time": fake.random_int(min=5, max=1000),  # Response time in milliseconds
        }
        return entry

    def ingest_data():
        for _ in range(1000):  # Generate 1000 log entries
            entry = generate_log_entry()
            es.index(index="web-logs", document=entry)  # Index the entry in "web-logs" index

    ingest_data()
except Exception as e:
    print(f"An error occurred: {e}")

