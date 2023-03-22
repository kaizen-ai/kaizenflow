

# Bitquery_uniswap_API_Code
# User gives start and stop timestamps


from typing import Any, Dict, List

import os
import pandas as pd
import requests

# API Key is BQYfQWbIU9aPXWtxpMAixOrC1fCLiIz3



# function for bitquery query
def run_bitquery_query(start_time: str,  limit: int) -> pd.DataFrame:
  # Query for the API
  query = """
  query{
    ethereum(network: ethereum) {
      dexTrades(
        options: {limit: %d, offset: %d, asc: "timeInterval.minute"}
        date: {since: "%s"},
        exchangeName: {in: ["Uniswap","Uniswap v2","Uniswap v3"]}
        )
        {
        timeInterval {
        minute(count: 1)
        }
      baseCurrency {
        symbol
        address
      }
      baseAmount(in: USD)
      quoteCurrency {
        symbol
        address
      }
      transaction {
        hash
        gas
        to {
          address
        }
        txFrom {
          address
        }    
        }
      quoteAmount(in: USD)
      trades: count
      quotePrice
      maximum_price: quotePrice(calculate: maximum)
      minimum_price: quotePrice(calculate: minimum)
      open_price: minimum(of: block, get: quote_price)
      close_price: maximum(of: block, get: quote_price)
      }
    }
  }
  """
  # This query gets us information on Ethereum from the 3 available Uniswap exchanges,
  # including the columns we will need in our future database

  # API endpoint and header
  endpoint = "https://graphql.bitquery.io/"
  headers = {"X-API-KEY": "BQYfQWbIU9aPXWtxpMAixOrC1fCLiIz3"}

  # Define an empty list to store the results
  results = []

  # initialize offset value
  offset = 0
  # Stream in data until there are no more results
  while True:
      # Construct the API query with the current offset
      fractured_query = query % (limit, offset, start_time)

      # Send the API request and get the response
      response = requests.post(endpoint, json={'query': fractured_query}, headers=headers)

      # Check if the API request was successful
      if response.status_code == 200:
          # Parse the response JSON
          response_json = response.json()

          # Extract the data from the response JSON
          data = response_json['data']['ethereum']['dexTrades']

          # Check if there are no more results
          if len(data) == 0:
              break

          # Append the data to the results list
          results += data

          # Update the offset
          offset += limit
      else:
          # If the API request failed, raise an exception and exit the loop
          raise Exception(
              "Query failed and return code is {}.      {}".format(
              response.status_code, query
                    )
              )

  # Normalize and convert the results list into a Pandas DataFrame
  df = json_to_df(results)
  
  return df



# Function for converting json to a dataframe
def json_to_df(data: List[Dict[Any, Any]]) -> pd.DataFrame:
  # normalize and set index to time_interval
  df = pd.json_normalize(data, sep="_")
  df = df.set_index("timeInterval_minute")
  return df

# Define the start time to retrieve data
start_time = '2023-03-22T00:00:00Z'

# Define the limit
limit = 25000

df = run_bitquery_query(start_time,limit)

print(df.head())




##### TODO 2 ##

# Split dataframe into table schema format for postgress
tran_token_info = df[["transaction_hash","baseCurrency_symbol","baseCurrency_address","quoteCurrency_symbol","quoteCurrency_address"]]
tran_wallet_info = df[["transaction_hash","transaction_to_address","transaction_txFrom_address"]]
tran_market_info = df[["transaction_hash","baseAmount","quoteAmount","quotePrice","maximum_price","minimum_price","open_price","close_price"]]
tran_metadata = df[["transaction_hash","trades","transaction_gas"]]

# Implement a flow to save the historical data from the source in a DB using a set of “tables”
# Explain the rationale for the choice of the DB

# Postgress schema
# Split df to match table schema and send to postgress db



### TODO 3 ##

# Implement a flow to download data from the source in the DB in real-time (in different tables)


## to grab timestamp of last entry in postgress db
# my_query = client.query("""
#   SELECT TIMESTAMP,
#     value,
#     card
#   FROM my_table
#   ORDER BY TIMESTAMP DESC
#   LIMIT 1
# """)