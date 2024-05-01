import requests
import json
import redis
from datetime import datetime, timedelta

# Initialize Redis
r = redis.Redis(host='localhost', port=6379, db=0)  # Ensure this matches your Redis setup

def fetch_historical_data(pair, since):
    url = "https://api.kraken.com/0/public/OHLC"
    params = {
        'pair': pair,
        'interval': 1440,  # Daily data
        'since': since
    }
    response = requests.get(url, params=params)
    print(response.json()) #this will print json file from the API
    if response.status_code == 200:
        data = response.json()
        return data['result'][pair], data['result']['last']
    else:
        print(f"Error occurred: {response.status_code}")
        print(response.text)
        return [], None

def save_data(data, filename):
    directory = '/app/data'  # Set the directory to the mounted volume
    filepath = os.path.join(directory, filename)
    with open(filepath, 'w') as file:
        json.dump(data, file, indent=4)
        
def main():
    pairs = {
        'BTCUSD': 'XXBTZUSD',
        'ETHUSD': 'XETHZUSD',
        'SOLUSD': 'SOLUSD',
        'LINKUSD': 'LINKUSD',
        'SOLUSD': 'SOLUSD',
        'ADAUSD': 'ADAUSD',
        'AXSUSD': 'AXSUSD',
        'MANAUSD': 'MANAUSD',
        'ENJUSD': 'ENJUSD',
        'RNDRUSD': 'RNDRUSD',
        'VTHOUSD': 'VTHOUSD',
        'USDTUSD': 'USDTUSD',
        'INJUSD': 'INJUSD',
        'MATICUSD': 'MATICUSD'

    }
    start_date = datetime.strptime('2010-01-01', '%Y-%m-%d')
    end_date = datetime.now()

    for symbol, kraken_symbol in pairs.items():
        all_data = []
        since = int(start_date.timestamp())

        while True:
            cache_key = f"historical_{kraken_symbol}_{since}"
            cached_data = r.get(cache_key)
            if cached_data:
                data, last = json.loads(cached_data)
            else:
                data, last = fetch_historical_data(kraken_symbol, since)
                if data:
                    r.setex(cache_key, 3600, json.dumps((data, last)))  # Cache for 1 hour

            if not data:
                break

            all_data.extend(data)
            if since == last or not last:
                break
            since = last

        if all_data:
            save_data(all_data, f'{symbol}_historical_data.json')

if __name__ == "__main__":
    main()



