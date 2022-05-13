"""
Generate example data and initiate client for access to it.

Import as:

import im_v2.common.data.client.realtime_clients_example as imvcdcrcex
"""
from typing import Optional

import pandas as pd

import core.finance as cofinanc
import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hsql as hsql
import im_v2.common.data.client as icdc
import im_v2.common.universe as ivcu


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
    Generate a dataframe with price features and fixed currency_pair and
    exchange_id.

    This imulates contents of DBs with crypto data, e.g. from Talos and CCXT.

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
    data["timestamp"] = data["timestamp"].apply(
        hdateti.convert_timestamp_to_unix_epoch
    )
    price_pattern = [101.0] * 5 + [100.0] * 5
    price = price_pattern * 4
    # All OHLCV columns are required for RealTimeMarketData.
    # TODO(Danya): Remove these columns and make MarketData vendor-agnostic.
    data["open"] = price
    data["high"] = price
    data["low"] = price
    data["close"] = price
    data["volume"] = 100
    # Add an extra feature1.
    feature_pattern = [1.0] * 5 + [-1.0] * 5
    feature = feature_pattern * 4
    data["feature1"] = feature
    # Add values necessary for `full_symbol`.
    data["currency_pair"] = "BTC_USDT"
    data["exchange_id"] = "binance"
    data = data[
        [
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "feature1",
            "currency_pair",
            "exchange_id",
            "timestamp_db",
        ]
    ]
    return data


class Example1SqlRealTimeImClient(icdc.SqlRealTimeImClient):
    def __init__(
        self,
        resample_1min: bool,
        db_connection: hsql.DbConnection,
        table_name: str,
        *,
        mode: Optional[str] = "market_data",
    ):
        vendor = "mock"
        super().__init__(
            resample_1min, db_connection, table_name=table_name, vendor=vendor
        )
        self._mode = mode

    @staticmethod
    def should_be_online() -> bool:
        return True

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
                lambda pd_timestamp: (pd_timestamp - delta)
            )
            data = data.set_index("timestamp")
        else:
            # TODO(Danya): Put a `data_client` mode for uses in testing.
            hdbg.dfatal(
                "Invalid mode='%s'. Correct modes: 'market_data'" % self._mode
            )
        return data


def get_example1_realtime_client(
    connection: hsql.DbConnection, resample_1min: bool
) -> Example1SqlRealTimeImClient:
    """
    Set up a real time Example1 SQL client.

    - Creates an Example1 table
    - Uploads example1 data
    - Creates a client connected to the given DB
    """
    # Create example table.
    table_name = "example1_marketdata"
    query = get_example1_create_table_query()
    connection.cursor().execute(query)
    # Create a data example and upload to local DB.
    data = create_example1_sql_data()
    hsql.copy_rows_with_copy_from(connection, data, table_name)
    # Initialize a client connected to the local DB.
    im_client = Example1SqlRealTimeImClient(resample_1min, connection, table_name)
    return im_client


# #############################################################################
# TestSqlRealTimeImClient
# #############################################################################


def get_example2_create_table_query() -> str:
    """
    Get SQL query to create a test table.

    The table schema corresponds to the OHLCV data and is used for
    testing.
    """
    query = """
    CREATE TABLE IF NOT EXISTS example2_marketdata(
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


def create_example2_sql_data():
    """
    Create an Example2 OHLCV dataframe.

    It recreates OHLCV data found in Talos and CCXT providers
    """
    test_data = pd.DataFrame(
        columns=[
            "id",
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "ticks",
            "currency_pair",
            "exchange_id",
            "end_download_timestamp",
            "knowledge_timestamp",
        ],
        # fmt: off
        # pylint: disable=line-too-long
        data=[
            [0, 1650637800000, 30, 40, 50, 60, 70, 80, "ETH_USDT", "binance", pd.Timestamp("2022-04-22"),
                pd.Timestamp("2022-04-22")],
            [1, 1650638400000, 31, 41, 51, 61, 71, 72, "BTC_USDT", "binance", pd.Timestamp("2022-04-22"),
                pd.Timestamp("2022-04-22")],
            [2, 1650639600000, 32, 42, 52, 62, 72, 73, "ETH_USDT", "binance", pd.Timestamp("2022-04-22"),
                pd.Timestamp("2022-04-22")],
            [3, 1650641400000, 34, 44, 54, 64, 74, 74, "BTC_USDT", "binance", pd.Timestamp("2022-04-22"),
                pd.Timestamp("2022-04-22")],
            [4, 1650645000000, 35, 45, 55, 65, 75, 75, "ETH_USDT", "binance", pd.Timestamp("2022-04-22"),
                pd.Timestamp("2022-04-22")],
            [5, 1650647400000, 36, 46, 56, 66, 76, 76, "BTC_USDT", "binance", pd.Timestamp("2022-04-22"),
                pd.Timestamp("2022-04-22")]
        ]
        # pylint: enable=line-too-long
        # fmt: on
    )
    return test_data


class Example2SqlRealTimeImClient(icdc.SqlRealTimeImClient):
    def __init__(
        self,
        resample_1min: bool,
        db_connection: hsql.DbConnection,
        table_name: str,
    ):
        vendor = "mock"
        super().__init__(
            resample_1min, db_connection, table_name=table_name, vendor=vendor
        )
        self._mode = "market_data"

    @staticmethod
    def should_be_online() -> bool:
        """
        The real-time system for Talos should always be online.
        """
        return True

    def _apply_normalization(
        self, data: pd.DataFrame, *, full_symbol_col_name: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Apply OHLCV normalization.

        - Add `start_timestamp` column in UTC timestamp format.
        - Add `end_timestamp` column in UTC timestamp format.
        - Add `asset_id` column which is result of mapping full_symbol to integer.
        - Drop extra columns.
        - The output looks like:
        ```
        open  high  low   close volume  start_timestamp          end_timestamp            asset_id
        0.825 0.826 0.825 0.825 18427.9 2022-03-16 2:46:00+00:00 2022-03-16 2:47:00+00:00 3303714233
        0.825 0.826 0.825 0.825 52798.5 2022-03-16 2:47:00+00:00 2022-03-16 2:48:00+00:00 3303714233
        ```
        """
        # Convert timestamp column with Unix epoch to timestamp format.
        data["timestamp"] = data["timestamp"].apply(
            hdateti.convert_unix_epoch_to_timestamp
        )
        ohlcv_columns = [
            "open",
            "high",
            "low",
            "close",
            "volume",
            full_symbol_col_name,
            "start_timestamp",
            "asset_id",
        ]
        # Add `asset_id` column using mapping on `full_symbol` column.
        data["asset_id"] = data[full_symbol_col_name].apply(
            ivcu.string_to_numerical_id
        )
        # Convert to int64 to keep NaNs alongside with int values.
        data["asset_id"] = data["asset_id"].astype(pd.Int64Dtype())
        # Generate `start_timestamp` from `end_timestamp` by substracting delta.
        delta = pd.Timedelta("1M")
        data["start_timestamp"] = data["timestamp"].apply(
            lambda pd_timestamp: (pd_timestamp - delta)
        )
        data = data.set_index("timestamp")
        # Rearrange the columns.
        data = data.loc[:, ohlcv_columns]
        return data


def get_example2_realtime_client(
    connection: hsql.DbConnection, resample_1min: bool
) -> Example1SqlRealTimeImClient:
    """
    Set up a real time Example2 SQL client.

    - Creates an Example2 table
    - Uploads example2 data
    - Creates a client connected to the given DB
    """
    # Create example table.
    table_name = "example2_marketdata"
    query = get_example2_create_table_query()
    connection.cursor().execute(query)
    # Create a data example and upload to local DB.
    data = create_example2_sql_data()
    hsql.copy_rows_with_copy_from(connection, data, table_name)
    # Initialize a client connected to the local DB.
    im_client = Example2SqlRealTimeImClient(resample_1min, connection, table_name)
    return im_client
