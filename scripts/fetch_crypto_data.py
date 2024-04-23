import requests
import json # Ensure you import json if you're using json.dumps for printing

def fetch_specific_cryptocurrency_data():
    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest'
    parameters = {
'symbol': 'BTC,ETH,USDT,AXS,MANA,ENJ,RNDR,INJ,VTHO,LINK,ADA,MATIC,QSP', # Add 'VARA' symbol if available
'convert': 'USD'
}
    headers = {
'Accepts': 'application/json',
'X-CMC_PRO_API_KEY': '2e06e691-732a-4d74-8e4b-9813b39a5ccf', # Replace 'your_api_key_here' with your actual API key
}

    # Making the HTTP request to CoinMarketCap API
    response = requests.get(url, headers=headers, params=parameters)

    # Checking if the request was successful
    if response.status_code == 200:
        # Parsing the JSON response
        data = response.json()
        return data
    else:
        # Handling errors (if any)
        print(f"Error occurred: {response.status_code}")
        return None

# Example usage
if __name__ == "__main__":
    data = fetch_specific_cryptocurrency_data()
    if data:
        # Printing the data in a nicely formatted JSON string
        print(json.dumps(data, indent=4))

    
