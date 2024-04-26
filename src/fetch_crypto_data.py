import requests
import os
import json

def fetch_specific_cryptocurrency_data():
    api_key = os.getenv('CMC_API_KEY')
    if not api_key:
        raise Exception("API key is not available. Set the CMC_API_KEY environment variable.")

    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest'
    parameters = {'symbol': 'BTC,ETH,SOL,AXS,MANA,ENJ,RNDR,INJ,VTHO,LINK,ADA,MATIC,QSP,VARA,USDT', 'convert': 'USD'}
    headers = {'Accept': 'application/json', 'X-CMC_PRO_API_KEY': api_key}

    response = requests.get(url, headers=headers, params=parameters)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error occurred: {response.status_code}")
        print(response.text)
        return None

def main():
    data = fetch_specific_cryptocurrency_data()
    if data:
        with open('cryptocurrency_data.json', 'w') as file:
            json.dump(data, file, indent=4)
        print(json.dumps(data, indent=4))

if __name__ == "__main__":
    main()
