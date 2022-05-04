"""
Generate example data and initiate client for access to it.
"""
from typing import Optional

import pandas as pd
import core.finance as cofinanc
import helpers.hdatetime as hdateti
import im_v2.common.data.client as icdc
import im_v2.common.universe as ivcu
import im_v2.common.db.db_utils as imvcddbut
import helpers.hsql as hsql
import helpers.hdbg as hdbg



def get_example1_create_table_query() -> str:
    """
    Get SQL query to create an Example1 table.

    The table schema corresponds to the OHLCV data.
    """
    query = """
    CREATE TABLE IF NOT EXISTS example1_marketdata(
            timestamp BIGINT,
            open NUMERIC,
            high NUMERIC,
            low NUMERIC,
            close NUMERIC,
            volume NUMERIC,
            feature1 NUMERIC,
            currency_pair VARCHAR(255) NOT NULL,
            exchange_id VARCHAR(255) NOT NULL,
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
    # TODO(Danya): all OHLCV columns are required for RealTimeMarketData.
    #  Remove and make MarketData vendor-agnostic.
    data["open"] = price
    data["high"] = price
    data["low"] = price
    data["close"] = price
    data["volume"] = 100
    # Add an extra feature1..
    feature_pattern = [1.0] * 5 + [-1.0] * 5
    feature = feature_pattern * 4
    data["feature1"] = feature
    # Add values necessary for `full_symbol`.
    data["currency_pair"] = "BTC_USDT"
    data["exchange_id"] = "binance"
    data = data[["timestamp",
                    "open",
                    "high", 
                    "low",
                    "close", 
                    "volume", 
                    "feature1", 
                    "currency_pair",
                    "exchange_id", 
                    "timestamp_db"]]
    return data

class ExampleSqlRealTimeImClient(icdc.SqlRealTimeImClient):
    def __init__(self,
        resample_1min: bool,
        db_connection: hsql.DbConnection,
        table_name: str, 
        db_helper: imvcddbut.TestImDbHelper,
        *,
        mode: Optional[str] = "market_data"):
        """
        Constructor.
        """
        vendor = "mock"
        super().__init__(resample_1min, db_connection, table_name=table_name, vendor=vendor)
        self._db_helper = db_helper
        self._mode = mode

    def _apply_normalization(
        self,
        data: pd.DataFrame,
        *,
        full_symbol_col_name: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Apply vendor-specific normalization.

        `market_data` mode:
            - Add `start_timestamp` column in UTC timestamp format.
            - Set `timestamp` as index
            - Add `asset_id` column which is result of mapping full_symbol to integer.
            - The output looks like:
        ```
        timestamp                 open ... volume  start_timestamp           asset_id
        2000-01-01 09:30:00-05:00 101.5    100     2000-01-01 09:29:00-05:00 3303714233
        2000-01-01 09:31:00-05:00 101.5    100     2000-01-01 09:30:00-05:00 3303714233
        ```
        """
        # Convert timestamp column with Unix epoch to timestamp format.
        data["timestamp"] = data["timestamp"].apply(
            hdateti.convert_unix_epoch_to_timestamp
        )
        full_symbol_col_name = self._get_full_symbol_col_name(
            full_symbol_col_name
        )
        if self._mode == "market_data":
            data["asset_id"] = data[full_symbol_col_name].apply(
                    ivcu.string_to_numerical_id
                )
            # Convert to int64 to keep NaNs alongside with int values.
            data["asset_id"] = data["asset_id"].astype(pd.Int64Dtype())
            # Generate `start_timestamp` from `end_timestamp` by substracting delta.
            delta = pd.Timedelta("1M")
            data["start_timestamp"] = data["timestamp"].apply(
                lambda pd_timestamp: (pd_timestamp - delta))
            data = data.set_index("timestamp")
        else:
            # TODO(Danya): Put a `data_client` mode for uses in testing.
            hdbg.dfatal(
                "Invalid mode='%s'. Correct modes: 'market_data'"
                % self._mode
            )
        return data

    def should_be_online():
        return True
    
    def tear_down(self) -> None:
        """
        Remove the local database.

        Needs to be called after each session to avoid
        tests that also use the local DB to crash.
        """
        #TODO(Danya): use a non-test DB environment.
        self._db_helper.tearDownClass()

def get_example1_realtime_client(resample_1min: bool) -> ExampleSqlRealTimeImClient:
    """
    Set up a real time SQL client.

    - Creates a local DB (in test environment)
    - Uploads test data
    - Creates a client connected to the local DB
    """
    # Initiate the temporary database.
    #  Note: using a test class to avoid duplicating code,
    #  since "local" stage DB will have the same credentials.
    # TODO(Danya): create a container separate from the test environment.
    db_helper = imvcddbut.TestImDbHelper()
    db_helper.setUpClass()
    # Get database connection.
    connection = db_helper.connection
    # Create example table.
    query = get_example1_create_table_query()
    connection.cursor().execute(query)
    # Create a data example and upload to local DB.
    data = create_example1_sql_data()
    hsql.copy_rows_with_copy_from(db_helper.connection, data, "example1_marketdata")
    # Initialize a client connected to the local DB.
    im_client = ExampleSqlRealTimeImClient(resample_1min, connection, "example1_marketdata", db_helper)
    return im_client
