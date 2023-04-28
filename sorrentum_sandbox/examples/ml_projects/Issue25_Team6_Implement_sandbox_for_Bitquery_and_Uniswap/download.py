"""
Extract part of the ETL and QA pipeline.

Import as:

import sorrentum_sandbox.examples.ml_projects.Issue25_Team6_Implement_sandbox_for_Bitquery_and_Uniswap.download as sisebido
"""

import logging
import os
import pandas as pd
import requests
from typing import Any, Dict, List
from dotenv import load_dotenv
import datetime as datetime 


import sorrentum_sandbox.common.download as ssandown

_LOG = logging.getLogger(__name__)


# #############################################################################
# Bitquery API Downloader
# #############################################################################


# function for bitquery query
def run_bitquery_query(start_time: str, end_time: str = None, live_flag: bool = False) -> ssandown.RawData:
    # Query for the API
    limit = 25000
    offset = 0

    time_format = '%Y-%m-%d %H:%M:%S'

    # Check for live_flag
    if live_flag:
        live_date = get_recent_timestamp()
        query_alter_1 = "since"
        query_alter_2 = "%s" % live_date

    # Alter query depending on if end_time is present
    elif end_time == None:
        query_alter_1 = "since"
        query_alter_2 = "%s" % start_time
    else:
        query_alter_1 = "between"
        query_alter_2 = "[%s, %s]" % (start_time,end_time)

    # GraphQL API query to get Uniswap DEX data
    query = """
    query{
    ethereum(network: ethereum) {
        dexTrades(
            options: {desc: ["block.height", "tradeIndex"], limit: %d, offset: %d}
            protocol: {is: "Uniswap"}
            date: {%s: "%s"},
        ) {
            timeInterval {
            minute
            }
            block {
            timestamp {
                time(format: "%s")
            }
            height
            }
            tradeIndex
            protocol
            exchange {
            fullName
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
    # Get API Key
    load_dotenv()
    api_key = os.environ.get('API_KEY')
    endpoint = "https://graphql.bitquery.io/"
    headers = {"X-API-KEY": api_key}



    # Define an empty list to store the results
    results = []

    # # # initialize offset value
    # offset = 0

    # Stream in data until there are no more results
    while True:
        # Construct the API query with the current offset
        fractured_query = query % (limit, offset,query_alter_1, query_alter_2,time_format)

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

            # # Update the offset
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

    # lowercase column names and convert to Raw Data
    df = df.rename(str.lower, axis='columns')
    _LOG.info(f"Downloaded data: \n\t {df.head()}")
    return ssandown.RawData(df)

# Function for converting json to a dataframe
def json_to_df(data: List[Dict[Any, Any]]) -> pd.DataFrame:
    # normalize and set index to time_interval
    df = pd.json_normalize(data, sep="_")
    # df = df.set_index("timeInterval_minute") 
    return df





def get_recent_timestamp() -> str:
  # database connection parameters
  host = 'localhost'
  port = '5432'
  dbname = 'db'
  user = 'postgres'
  password = 'postgres'

  conn = psycopg2.connect(
      host=host,
      port=port,
      dbname=dbname,
      user=user,
      password=password
  )
  # Create a cursor to execute SQL queries
  cur = conn.cursor()

  # Execute a SQL query to retrieve the last row of the table
  cur.execute("SELECT * FROM uniswap_table ORDER BY timeinterval_minute DESC LIMIT 1")

  # Extract the timestamp from the last row
  result = cur.fetchone()
  print(result)
  last_timestamp = result[3]

  # Convert the timestamp to a datetime object if needed
  last_datetime = datetime.strptime(last_timestamp, '%Y-%m-%d %H:%M:%S')

  # Format the datetime object in ISO-8601 format
  iso_timestamp = last_datetime.strftime('%Y-%m-%dT%H:%M:%SZ')

  print(iso_timestamp)
  # Close the cursor and the connection
  cur.close()
  limit = 10

  # using the timestamp start new query
  df = run_bitquery_query(iso_timestamp,limit)


