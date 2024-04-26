import redis
import json

# Connect to Redis
r = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)

def cache_data(key, data):
    # Cache data in Redis
    r.set(key, json.dumps(data))

def fetch_cached_data(key):
    # Fetch data from Redis
    data = r.get(key)
    if data:
        return json.loads(data)
    return None

# Example usage
key = "user_profile"
data = {"name": "Farhad Abasahl", "UID": 113959479}
cache_data(key, data)  # Cache data
fetched_data = fetch_cached_data(key)  # Retrieve cached data
print(fetched_data)