import requests
import pandas as pd
import os
def fetch_cryptocurrency_data(api_key):
	url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
	headers = {
		'Accepts': 'application/json',
		'X-CMC_PRO_API_KEY': api_key,
	}
	parameters = {
		'start': '1',
		'limit': '100', # Adjust the limit as needed
		'convert': 'USD'
	}

	response = requests.get(url, headers=headers, params=parameters)
	response_json = response.json()

	# Check for a successful request
	if response.status_code == 200:
		return response_json['data']
	else:
		print("Failed to fetch data:", response_json.get('status', {}).get('error_message', 'Unknown error'))
		return []

def main():	
	API_KEY = '2e06e691-732a-4d74-8e4b-9813b39a5ccf' # Replace with your actual API key
	data = fetch_cryptocurrency_data(API_KEY)
	if data:
		df = pd.DataFrame(data)
		df.to_csv('data/cryptocurrencies.csv', index=False)
		print("Data fetched and saved successfully.")
	else:
		print("No data to save.")

if __name__ == "__main__":
	main()

def main():
	API_KEY = os.getenv('CMC_API_KEY')
