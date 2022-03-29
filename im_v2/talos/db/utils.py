"""
Utilities for working with Talos related database tables.

Import as:

import im_v2.talos.db.utils as imvtadbut
"""

import logging

_LOG = logging.getLogger(__name__)


def get_talos_ohlcv_create_table_query() -> str:
    """
    Get SQL query to create Talos OHLCV table.
    """
    query = """
    CREATE TABLE IF NOT EXISTS talos_ohlcv(
            id SERIAL PRIMARY KEY,
            timestamp BIGINT NOT NULL,
            open NUMERIC,
            high NUMERIC,
            low NUMERIC,
            close NUMERIC,
            volume NUMERIC,
            ticks NUMERIC,
            currency_pair VARCHAR(255) NOT NULL,
            exchange_id VARCHAR(255) NOT NULL,
            end_download_timestamp TIMESTAMP,
            knowledge_timestamp TIMESTAMP
            )
            """
    return query
