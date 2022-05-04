"""
Generate example data and initiate client for access to it.
"""

import pandas as pd
import core.finance as cofinanc
import helpers.hdatetime as hdateti
import im_v2.common.data.client as icdc
from typing import Optional
import im_v2.common.db.db_utils as imvcddbut
import helpers.hsql as hsql


def get_example1_create_table_query() -> str:
    """
    Get SQL query to create an Example1 table.

    The table schema corresponds to the 
    """
    query = """
    CREATE TABLE IF NOT EXISTS example1_marketdata(
            id SERIAL PRIMARY KEY,
            timestamp BIGINT NOT NULL,
            close NUMERIC,
            asset_id BIGINT,
            volume NUMERIC,
            feature1 NUMERIC,
            timestamp_db TIMESTAMP
            )
            """
    return query


def create_example1_sql_data() -> pd.DataFrame:
    """
    Generate a dataframe with price features and fixed currency_pair and exchange_id.

    Simulates contents of DBs with crypto data, e.g. from Talos and CCXT.

    Output example:

    ```
    timestamp  close  volume  feature1 currency_pair exchange_id              timestamp_db
    946737060000  101.0     100       1.0      BTC_USDT     binance 2000-01-01 09:31:00-05:00
    946737120000  101.0     100       1.0      BTC_USDT     binance 2000-01-01 09:32:00-05:00
    946737180000  101.0     100       1.0      BTC_USDT     binance 2000-01-01 09:33:00-05:00
    ```
    """
    idx = pd.date_range(
        start=pd.Timestamp("2000-01-01 09:31:00-05:00", tz="America/New_York"),
        end=pd.Timestamp("2000-01-01 10:10:00-05:00", tz="America/New_York"),
        freq="T",
    )
    bar_duration = "1T"
    bar_delay = "0T"
    data = cofinanc.build_timestamp_df(idx, bar_duration, bar_delay)
    data = data.reset_index().rename({"index": "timestamp"}, axis=1)
    data["timestamp"] = data["timestamp"].apply(hdateti.convert_timestamp_to_unix_epoch)
    price_pattern = [101.0] * 5 + [100.0] * 5
    price = price_pattern * 4
    data["close"] = price
    data["volume"] = 100
    feature_pattern = [1.0] * 5 + [-1.0] * 5
    feature = feature_pattern * 4
    data["feature1"] = feature
    data["currency_pair"] = "BTC_USDT"
    data["exchange_id"] = "binance"
    data = data[["timestamp","close", "volume", "feature1", "currency_pair", "exchange_id", "timestamp_db"]]
    return data

class ExampleSqlRealTimeImClient(icdc.SqlRealTimeImClient):
    def __init__(resample_1min: bool, db_connection: hsql.DbConnection, table_name: str, db_helper: imvcddbut.TestImDbHelper):
        vendor = "mock"
        super().__init__(resample_1min, db_connection, table_name=table_name, vendor=vendor)
        self.db_helper = db_helper

    def _apply_normalization(
        self,
        data: pd.DataFrame,
        *,
        full_symbol_col_name: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Apply vendor-specific normalization.
        """
        pass

    def should_be_online():
        return True

def get_example1_realtime_client():
    """
    
    """
    db_helper = imvcddbut.TestImDbHelper()
    db_helper.setUpClass()
    connection = db_helper.connection
    query = get_example1_create_table_query()
    connection.cursor().execute(query)
    data = create_example1_sql_data()
    hsql.copy_rows_with_copy_from(db_helper.connection, data, "example1_marketdata")
    return im_client