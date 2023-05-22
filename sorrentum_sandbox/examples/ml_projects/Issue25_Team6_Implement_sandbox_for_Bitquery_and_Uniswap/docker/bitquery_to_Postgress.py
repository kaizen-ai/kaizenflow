# Bitquery_uniswap_API_Code
# User gives start and stop timestamps


import os
from datetime import datetime
from io import StringIO
from typing import Any, Dict, List

import pandas as pd
import psycopg2
import requests
from sqlalchemy import create_engine

# function for bitquery query
def run_bitquery_query(start_time: str, limit: int) -> pd.DataFrame:
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
    headers = {"X-API-KEY": ""}

    # Define an empty list to store the results
    results = []

    # initialize offset value
    offset = 0
    # Stream in data until there are no more results
    while True:
        # Construct the API query with the current offset
        fractured_query = query % (limit, offset, start_time)

        # Send the API request and get the response
        response = requests.post(
            endpoint, json={"query": fractured_query}, headers=headers
        )

        # Check if the API request was successful
        if response.status_code == 200:
            # Parse the response JSON
            response_json = response.json()

            # Extract the data from the response JSON
            data = response_json["data"]["ethereum"]["dexTrades"]

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
start_time = "2023-03-22T00:00:00Z"

# Define the limit
limit = 25000


# Commented out for debugging - Query was taking too long
df = run_bitquery_query(start_time, limit)

# print(df.head())

# df.to_csv("bitquery_raw.csv")


# df = pd.read_csv(r"C:\Users\jrfie\Documents\Git_Repository\UMD_Courses\DATA605\sorrentum\projects\Issue25_Team6_Implement_sandbox_for_Bitquery_and_Uniswap\docker\bitquery_raw.csv")

# def build_postgress_table(df:pd.DataFrame)
# Split dataframe into table schema format for postgress
tran_token_info = df[
    [
        "transaction_hash",
        "baseCurrency_symbol",
        "baseCurrency_address",
        "quoteCurrency_symbol",
        "quoteCurrency_address",
    ]
]
tran_wallet_info = df[
    ["transaction_hash", "transaction_to_address", "transaction_txFrom_address"]
]
tran_market_info = df[
    [
        "transaction_hash",
        "baseAmount",
        "quoteAmount",
        "quotePrice",
        "maximum_price",
        "minimum_price",
        "open_price",
        "close_price",
    ]
]
tran_metadata = df[["transaction_hash", "trades", "transaction_gas"]]


print(tran_token_info.head())
print(len(tran_token_info))


# tables_and_dfs = [
#   {
#     "table_name":"tran_token_info",
#     "df":tran_token_info
#   },
#   {
#     "table_name":"tran_wallet_info",
#     "df":tran_wallet_info
#   },
#   {
#     "table_name":"tran_market_info",
#     "df":tran_market_info
#   },
#   {
#     "table_name":"tran_metadata",
#     "df":tran_metadata
#   }
#   ]

# database connection parameters
host = "localhost"
port = "5432"  # this might be 8001
dbname = "db"
user = "user"
password = "password"

# connection to the postgress database
conn = psycopg2.connect(
    host=host, port=port, dbname=dbname, user=user, password=password
)

# # Use SQLAlchemy to create the table

engine = create_engine(
    "postgresql://user:password@localhost:5432/db", fast_executemany=True
)

# # # upload tables to postgress server
tran_token_info.to_sql(
    "tran_token_info",
    engine,
    index=False,
    if_exists="replace",
    method="multi",
    chunksize=3000,
)
tran_wallet_info.to_sql(
    "tran_wallet_info",
    engine,
    index=False,
    if_exists="replace",
    method="multi",
    chunksize=3000,
)
tran_market_info.to_sql(
    "tran_market_info",
    engine,
    index=False,
    if_exists="replace",
    method="multi",
    chunksize=3000,
)
tran_metadata.to_sql(
    "tran_token_info",
    engine,
    index=False,
    if_exists="replace",
    method="multi",
    chunksize=3000,
)


# # Iterate over the list and insert each dataframe into its respective table
# for item in tables_and_dfs:
#     table_name = item["table_name"]
#     df = item["df"]

#     # Create a new table in the database
#     with conn.cursor() as cur:
#         if table_name == "tran_token_info":
#           cur.execute(f"""
#               CREATE TABLE IF NOT EXISTS tran_token_info (
#                   transaction_hash bigint,
#                   baseCurrency_symbol TEXT,
#                   baseCurrency_address bigint,
#                   quoteCurrency_symbol TEXT,
#                   quoteCurrency_address bigint
#               );
#           """)
#         elif table_name == "tran_wallet_info":
#           cur.execute(f"""
#           CREATE TABLE IF NOT EXISTS tran_wallet_info (
#               transaction_hash bigint,
#               transaction_to_address bigint,
#               transaction_txFrom_address bigint,
#               );
#           """)
#         elif table_name == "tran_market_info":
#           cur.execute(f"""
#           CREATE TABLE IF NOT EXISTS tran_market_info (
#               transaction_hash bigint,
#               baseAmount FLOAT,
#               quoteAmount FLOAT,
#               maximum_price FLOAT,
#               minimum_price FLOAT,
#               open_price FLOAT,
#               close_price FLOAT,
#               );
#           """)
#         elif table_name == "tran_metadata":
#           cur.execute(f"""
#           CREATE TABLE IF NOT EXISTS tran_metadata (
#               transaction_hash TEXT,
#               trades INTEGER,
#               transaction_gas INTEGER,
#               );
#           """)
#     # Convert the dataframe to a string buffer
#     buffer = StringIO()
#     df.to_csv(buffer, index=False, header=False)
#     buffer.seek(0)

#     # Insert the data into the new table
#     with conn.cursor() as cur:
#         cur.copy_from(buffer, table_name, sep=",")
#         conn.commit()

# # Close the connection
# conn.close()


# # Create a new table in the database
# with conn.cursor() as cur:
#     cur.execute(f"""
#         CREATE TABLE IF NOT EXISTS tran_token_info (
#             transaction_hash SERIAL PRIMARY KEY,
#             transaction_to_address TEXT,
#             baseCurrency_address TEXT,
#             quoteCurrency_symbol TEXT,
#             quoteCurrency_address TEXT
#         );
#     """)

#     cur.execute(f"""
#     CREATE TABLE IF NOT EXISTS tran_wallet_info (
#         transaction_hash SERIAL PRIMARY KEY,
#         transaction_to_address TEXT,
#         transaction_txFrom_address TEXT,
#         );
#     """)

#     cur.execute(f"""
#     CREATE TABLE IF NOT EXISTS tran_market_info (
#         transaction_hash SERIAL PRIMARY KEY,
#         baseAmount FLOAT,
#         quoteAmount FLOAT,
#         maximum_price FLOAT,
#         minimum_price FLOAT,
#         open_price FLOAT,
#         close_price FLOAT,
#         );
#     """)

#     cur.execute(f"""
#     CREATE TABLE IF NOT EXISTS tran_metadata (
#         transaction_hash SERIAL PRIMARY KEY,
#         trades INTEGER,
#         transaction_gas INTEGER,
#         );
#     """)

# # # Convert the dataframe to a string buffer
# # buffer = StringIO()
# # tran_token_info.to_csv(buffer, index=False, header=False)
# # buffer.seek(0)

# # # Insert the data into the new table
# # with conn.cursor() as cur:
# #     cur.copy_from(buffer, "tran_token_info", sep=",")
# #     conn.commit()

# # # Convert the dataframe to a string buffer
# # buffer = StringIO()
# # tran_token_info.to_csv(buffer, index=False, header=False)
# # buffer.seek(0)

# # # Insert the data into the new table
# # with conn.cursor() as cur:
# #     cur.copy_from(buffer, "tran_token_info", sep=",")
# #     conn.commit()


# print("here")

# # Create a cursor to execute SQL queries
# cur = conn.cursor()
# # Execute a SQL query to retrieve the last row of the table
# cur.execute("SELECT * FROM tran_token_info")
# result = cur.fetchall()
# print(result)

# # Execute a SQL query to retrieve the last row of the table
# cur.execute("SELECT * FROM tran_token_info ORDER BY index DESC LIMIT 1")

# # Extract the timestamp from the last row
# result = cur.fetchone()
# last_timestamp = result[0]  # Assuming timestamp is the index

# # Convert the timestamp to a datetime object if needed
# last_datetime = datetime.strptime(last_timestamp, '%Y-%m-%d %H:%M:%S')

# # Format the datetime object in ISO-8601 format
# iso_timestamp = last_datetime.strftime('%Y-%m-%dT%H:%M:%SZ')

# print(iso_timestamp)
# # Close the cursor and the connection
# cur.close()

# # Close the connection
# conn.close()


# # create table in database
# # create_table_query = '''CREATE TABLE IF NOT EXISTS tran_token_info (
# #                             transaction_hash varchar(255),
# #                             age int
# #                         )'''

# # Implement a flow to save the historical data from the source in a DB using a set of “tables”
# # Explain the rationale for the choice of the DB

# # Postgress schema
# # Split df to match table schema and send to postgress db


# ### TODO 3 ##

# # Implement a flow to download data from the source in the DB in real-time (in different tables)


# ## to grab timestamp of last entry in postgress db
# # my_query = client.query("""
# #   SELECT TIMESTAMP,
# #     value,
# #     card
# #   FROM my_table
# #   ORDER BY TIMESTAMP DESC
# #   LIMIT 1
# # """)
