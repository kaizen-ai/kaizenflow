from typing import Any, Optional

import pandas as pd
import psycopg2 as psycop
import psycopg2.extras as extras

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import sorrentum_sandbox.common.client as sinsacli
import sorrentum_sandbox.common.download as sinsadow
import sorrentum_sandbox.common.save as sinsasav

def get_ohlcv_spot_download_1min_create_table_query() -> str:
    query = """
    CREATE TABLE IF NOT EXISTS binance_ohlcv_spot_downloaded_1min(
            id SERIAL PRIMARY KEY,
            timestamp BIGINT NOT NULL,
            open NUMERIC,
            high NUMERIC,
            low NUMERIC,
            close NUMERIC,
            volume NUMERIC,
            currency_pair VARCHAR(255) NOT NULL,
            end_download_timestamp TIMESTAMP WITH TIME ZONE,
            knowledge_timestamp TIMESTAMP WITH TIME ZONE default CURRENT_TIMESTAMP,
            UNIQUE(timestamp, currency_pair)
            )
            """
    return query

def get_ohlcv_spot_resampled_5min_create_table_query() -> str:
    """
    Get SQL query to create Binance OHLCV resampled model table.
    """
    query = """
    CREATE TABLE IF NOT EXISTS binance_ohlcv_spot_resampled_5min(
            id SERIAL PRIMARY KEY,
            timestamp BIGINT NOT NULL,
            open NUMERIC,
            high NUMERIC,
            low NUMERIC,
            close NUMERIC,
            volume NUMERIC,
            currency_pair VARCHAR(255) NOT NULL,
            end_download_timestamp TIMESTAMP WITH TIME ZONE,
            knowledge_timestamp TIMESTAMP WITH TIME ZONE default CURRENT_TIMESTAMP,
            UNIQUE(timestamp, currency_pair)
            )
            """
    return query

def get_db_connection() -> Any:

    connection = psycop.connect(
        host="host.docker.internal",
        dbname="kaiko database",
        port="5432",
        user="postgres",
        password="Oliver1999",
    )
    connection.autocommit = True
    return connection
    
