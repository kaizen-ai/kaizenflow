from elasticsearch import Elasticsearch
from faker import Faker
import json
import random
from datetime import datetime, timedelta

fake = Faker()

try:
    es = Elasticsearch(
        "https://localhost:9200",
        #api_key="aHZMcFJZOEJxNGJsQVJBWDdfQTg6N05PYllRb0xSNXFzYWxnSWMwaGVXUQ==",#verify_certs=False
        basic_auth=("elastic", "ilovethis"),
        ca_certs='/Users/berksomer/src/kaizenflow/sorrentum_sandbox/spring2024/SorrTask833_ElasticSearch_WebTraffic/ca.crt'
        #verify_certs=False
    )
    
    current_time = datetime.now()
    
    def generate_log_entry(seconds):
        # Generate a fake log entry within a time interval between now(current_time) and 3600 seconds earlier (1hr)
        time_offset = timedelta(seconds=random.randint(0, seconds))
        entry_time = current_time - time_offset

        entry = {
            "timestamp": entry_time.strftime("%Y-%m-%dT%H:%M:%S"),
            "ip": fake.ipv4(),
            "method": random.choices(["GET", "POST", "DELETE", "PUT"], weights=[65, 22, 8, 5],k=1)[0],
            "endpoint": fake.uri_path(),
            "response_code": random.choices([200, 201, 404, 500], weights=[50, 30, 15, 5],k=1)[0],
            "response_time": fake.random_int(min=5, max=1000)
        }
        return entry

    def ingest_data():
        for i in range(4000):
        # Generate this fake log 4000 times
        # With this logs are randomly distributed between now and a time interval before
            entry = generate_log_entry(3600)
            response = es.index(index="web-logs", document=entry)
            print(response)

    ingest_data()

except Exception as e:
    print(f"An error occurred: {e}")


