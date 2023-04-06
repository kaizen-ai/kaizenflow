from typing import Any, Optional
import pandas as pd
import psycopg2 as psycop
import psycopg2.extras as extras
import requests
from pycoingecko import CoinGeckoAPI
import json

def get_db_connection() -> Any:
    """
    Retrieve connection to the Postgres DB inside the Sorrentum data node.
    The parameters must match the parameters set up in the Sorrentum
    data node docker-compose.
    """
    connection = psycop.connect(
        host="localhost",
        dbname="postgres",
        port=5432,
        user="postgres",
        password="postgres",
    )
    connection.autocommit = True

    return connection


def get_coingecko_create_table_query() -> str:
    """
    Get SQL query to create coingecko table.
    """
    query = """
                CREATE TABLE IF NOT EXISTS coingecko_data
                (
                    id INTEGER PRIMARY KEY, 
                    symbol varchar(225),
                    name varchar(255),
                    current_price FLOAT,
                    market_cap FLOAT,
                    market_cap_rank INTEGER,
                    total_volume FLOAT,
                    high_24h FLOAT,
                    low_24h FLOAT
                );
            """
    return query


def get_coingecko_drop_query() -> str:
    """
    Get SQL query to drop coingecko table.
    """
    query = "drop table coingecko_data"

    return query



# def get_coingecko_table_fetch_query(id) -> str:

#     query = ""
#     query = query + "SELECT * FROM coingecko_data WHERE id = %s"

#     return query


def fetch_data(url):
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 100,
        "page": 1,
        "sparkline" : False
    }
    response = requests.get(url, params=params)
    if response.status_code != 200:
        raise ValueError(f"Failed to fetch data from {url}. Error {response.status_code}")
    data = response.json()
    return data

if __name__ == "__main__":
    connection_obj = get_db_connection()
    print("Connection object: ", connection_obj)

    table_creation_query = get_coingecko_create_table_query()
    table_deletion_query = get_coingecko_drop_query()

    print("query to print table: \n", table_creation_query)
    url = 'https://api.coingecko.com/api/v3/coins/markets'
    data = fetch_data(url)
    cursor = connection_obj.cursor()
    cursor.execute(table_creation_query)
    for item in data:
        cursor.execute("INSERT INTO coingecko_data (id, symbol, name, current_price, market_cap, market_cap_rank, total_volume, high_24h, low_24h) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
               (item['id'], item['symbol'], item['name'], item['current_price'], item['market_cap'],
                item['market_cap_rank'], item['total_volume'], item['high_24h'], item['low_24h']))

    cursor.commit()
    connection_obj.close()
    print("connection closed")