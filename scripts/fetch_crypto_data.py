import os
import json
import shutil
import subprocess
import logging
from datetime import datetime
import requests
import redis

# Setup logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Connect to Redis
r = redis.Redis(host='redis', port=6379, db=0)

def fetch_historical_data(pair, since):
    url = "https://api.kraken.com/0/public/OHLC"
    params = {
        'pair': pair,
        'interval': 1440,
        'since': since
    }
    response = requests.get(url, params=params)
    logging.debug(response.json())
    if response.status_code == 200:
        data = response.json()
        return data['result'][pair], data['result']['last']
    else:
        logging.error(f"Error occurred: {response.status_code}")
        logging.error(response.text)
        return [], None

def save_data(data, filename):
    directory = '/app/data'
    filepath = os.path.join(directory, filename)
    with open(filepath, 'w') as file:
        json.dump(data, file, indent=4)
        logging.info(f"Data saved to {filepath}")
        return filepath

def configure_git():
    try:
        subprocess.run(["git", "config", "--global", "user.email", "farhad@umd.edu"])
        subprocess.run(["git", "config", "--global", "user.name", "Farhad Abasahl"])
        logging.info("Git configured successfully")
    except Exception as e:
        logging.error(f"Failed to configure Git: {str(e)}")

def save_to_github(filepath):
    try:
        # Configure Git
        configure_git()

        # Copy file from /app/data to /home/jovyan/work
        host_filepath = filepath.replace('/app/data', '/home/jovyan/work')
        shutil.copy(filepath, host_filepath)
        os.remove(filepath)

        # Change to the appropriate directory
        os.chdir("/home/jovyan/work")

        # Add the file
        subprocess.run(["git", "add", host_filepath])

        # Commit the changes
        subprocess.run(["git", "commit", "-m", f"Added {os.path.basename(filepath)}"])

        # Push the changes
        github_pat = os.getenv("GITHUB_PAT")
        push_url = f"https://farhad:{github_pat}@github.com/Farhad1969/sorrentum.git"
        result = subprocess.run(
            ["git", "push", push_url],
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
        )
        if result.returncode != 0:
            logging.error(f"Failed to push {filepath} to GitHub: {result.stderr}")
        else:
            logging.info(f"Successfully pushed {host_filepath} to GitHub")
    except Exception as e:
        logging.error(f"Failed to save {filepath} to GitHub: {str(e)}")


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
                    r.setex(cache_key, 3600, json.dumps((data, last)))

            if not data:
                break

            all_data.extend(data)
            if since == last or not last:
                break
            since = last

        if all_data:
            filepath = save_data(all_data, f'{symbol}_historical_data.json')
            save_to_github(filepath)

if __name__ == "__main__":
    main()
