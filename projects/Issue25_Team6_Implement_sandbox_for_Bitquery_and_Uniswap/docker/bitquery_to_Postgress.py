

# Bitquery_uniswap_API_Code
# User gives start and stop timestamps


from typing import Any, Dict, List

import os
import pandas as pd
import requests

# API Key is BQYfQWbIU9aPXWtxpMAixOrC1fCLiIz3



# Bitquery Query request
def run_query(query: str):
    headers = {"X-API-KEY": "BQYfQWbIU9aPXWtxpMAixOrC1fCLiIz3"}
    request = requests.post(
        "https://graphql.bitquery.io/", json={"query": query}, headers=headers
    )
    if request.status_code == 200:
        return request.json()
    else:
        raise Exception(
            "Query failed and return code is {}.      {}".format(
                request.status_code, query
            )
        )


# Function for converting json to a dataframe
def json_to_df(data: List[Dict[Any, Any]]) -> pd.DataFrame:
    """
    Transform the data to Dataframe and set the time index.
    """
    df = pd.json_normalize(data, sep="_")
    df = df.set_index("timeInterval_minute")
    return df


# Query for the API
query = """
query{
  ethereum(network: ethereum) {
    dexTrades(
      options: {limit: 25000, offset: %d, asc: "timeInterval.minute"}
      date: {since: "2023-02-22"}
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

limit = 25000
offset = 0
data_dfs = []

for i in range(3):
    # Error message above says there are 61k rows, so 3 iterations by 25k would be enough.
    formatted_query = query % offset
    result = run_query(formatted_query)
    df = json_to_df(result["data"]["ethereum"]["dexTrades"])
    data_dfs.append(df)
    offset += 25000

full_data = pd.concat(data_dfs)


df = json_to_df(result["data"]["ethereum"]["dexTrades"])
df.head()



# Push historical transaction data to the database:
# def create_Table_query() -> str:
## Insert dataframe to database upload command here
#
# CREATE TABLE IF NOT EXISTS __TABLENAME__ (
# 
# id SERIAL PRIMARY KEY,
# timestamp BIGINT NOT NULL
# 
# 
# 
# )