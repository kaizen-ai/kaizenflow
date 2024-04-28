import requests
import os
import json
import redis
from datetime import datetime

# Initialize Redis
r = redis.Redis(host='redis', port=6379, db=0)

def fetch_specific_cryptocurrency_data():
    api_key = os.getenv('CMC_API_KEY')
    if not api_key:
        raise Exception("API key is not available. Set the CMC_API_KEY environment variable.")

    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest'
    parameters = {'symbol': 'BTC,ETH,SOL,AXS,MANA,ENJ,RNDR,INJ,VTHO,LINK,ADA,MATIC,QSP,VARA,USDT', 'convert': 'USD'}
    headers = {'Accept': 'application/json', 'X-CMC_PRO_API_KEY': api_key}

    # Check Redis cache before API request
    cache_key = f"cryptocurrency_data_{parameters['symbol']}"
    cached_data = r.get(cache_key)
    if cached_data:
        return json.loads(cached_data)

    response = requests.get(url, headers=headers, params=parameters)
    if response.status_code == 200:
        data = response.json()
        # Cache data with a timeout (e.g., 1 hour = 3600 seconds)
        r.set(cache_key, json.dumps(data), ex=3600)
        return data
    else:
        print(f"Error occurred: {response.status_code}")
        print(response.text)
        return None

def save_data(data, filename='cryptocurrency_data.json'):
    try:
        with open(filename, 'r') as file:
            existing_data = json.load(file)
    except (FileNotFoundError, json.JSONDecodeError):
        existing_data = {}

    timestamp = datetime.now().isoformat()

    if 'data' not in existing_data:
        existing_data['data'] = {}
    for symbol, details in data['data'].items():
        if symbol not in existing_data['data']:
            existing_data['data'][symbol] = []
        details['timestamp'] = timestamp
        existing_data['data'][symbol].append(details)

    with open(filename, 'w') as file:
        json.dump(existing_data, file, indent=4)

def main():
    data = fetch_specific_cryptocurrency_data()
    if data:
        save_data(data)

if __name__ == "__main__":
    main()
