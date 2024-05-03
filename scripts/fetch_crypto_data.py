import requests
import json
import os
import redis
import logging
from datetime import datetime, timedelta

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

try:
    # Initialize Redis
    r = redis.Redis(host='redis', port=6379, db=0)  # Adjust host if running outside Docker

    def fetch_historical_data(pair, since):
        url = "https://api.kraken.com/0/public/OHLC"
        params = {
            'pair': pair,
            'interval': 1440,  # Daily data
            'since': since
        }
        response = requests.get(url, params=params)
        logging.debug(response.json())  # Log API response
        if response.status_code == 200:
            data = response.json()
            return data['result'][pair], data['result']['last']
        else:
            logging.error(f"Error occurred: {response.status_code}")
            logging.error(response.text)
            return [], None

    def save_data(data, filename):
        directory = '/app/data'  # Set the directory to the mounted volume
        filepath = os.path.join(directory, filename)
        with open(filepath, 'w') as file:
            json.dump(data, file, indent=4)
            logging.info(f"Data saved to {filepath}")
        
    def main():
        pairs = {
            'BTCUSD': 'XXBTZUSD',
            'ETHUSD': 'XETHZUSD',
            'SOLUSD': 'SOLUSD',
            'LINKUSD': 'LINKUSD',
            'ADAUSD': 'ADAUSD',
            'AXSUSD': 'AXSUSD',
            'MANAUSD': 'MANAUSD',
            'ENJUSD': 'ENJUSD',
            'RNDRUSD': 'RNDRUSD',
        #   'VTHOUSD': 'VTHOUSD',
        #   'USDT': 'USDTUSD',
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

except redis.ConnectionError as e:
    logging.error(f"Failed to connect to Redis: {str(e)}")
except Exception as e:
    logging.error(f"An unexpected error occurred: {str(e)}")
