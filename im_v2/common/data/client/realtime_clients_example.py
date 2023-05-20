"""
Generate example data and initiate client for access to it.

Import as:

import im_v2.common.data.client.realtime_clients_example as imvcdcrcex
"""

import pandas as pd

import core.finance as cofinanc
import helpers.hdatetime as hdateti
import helpers.hsql as hsql
import helpers.hsql_implementation as hsqlimpl
import im_v2.common.data.client as icdc

# #############################################################################
# TestSqlRealTimeImClient
# #############################################################################


def _get_mock1_create_table_query() -> str:
    """
    Get SQL query to create a Mock1 table.

    The table schema corresponds to the OHLCV data.
    """
    query = """
    CREATE TABLE IF NOT EXISTS mock1_marketdata(
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


def _create_mock1_sql_data() -> pd.DataFrame:
    """
    Generate a dataframe with price features and fixed currency_pair and
    exchange_id.

    This simulates contents of DBs with crypto data, e.g. from Talos and CCXT.

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


# #############################################################################
# Mock1SqlRealTimeImClient
# #############################################################################


# TODO(Dan): Consider joining `Mock1SqlRealTimeImClient` and `MockSqlRealTimeImClient`.
class Mock1SqlRealTimeImClient(icdc.SqlRealTimeImClient):
    def __init__(
        self,
        resample_1min: bool,
        db_connection: hsql.DbConnection,
        table_name: str,
    ):
        vendor = "mock"
        super().__init__(vendor, resample_1min, db_connection, table_name)

    @staticmethod
    def should_be_online() -> bool:
        return True


def get_mock1_realtime_client(
    connection: hsql.DbConnection, *, resample_1min: bool = False
) -> Mock1SqlRealTimeImClient:
    """
    Set up a real time Mock1 SQL client.

    - Creates a Mock1 table
    - Uploads mock1 data
    - Creates a client connected to the given DB
    """
    # Create example table.
    table_name = "mock1_marketdata"
    query = _get_mock1_create_table_query()
    connection.cursor().execute(query)
    # Create a data example and upload to local DB.
    data = _create_mock1_sql_data()
    hsql.copy_rows_with_copy_from(connection, data, table_name)
    # Initialize a client connected to the local DB.
    im_client = Mock1SqlRealTimeImClient(resample_1min, connection, table_name)
    return im_client


# #############################################################################
# TestSqlRealTimeImClient
# #############################################################################


def _get_mock2_create_table_query() -> str:
    """
    Get SQL query to create a test table.

    The table schema corresponds to the OHLCV data and is used for
    testing.
    """
    query = """
    CREATE TABLE IF NOT EXISTS mock2_marketdata(
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
            end_download_timestamp TIMESTAMP WITH TIME ZONE,
            knowledge_timestamp TIMESTAMP WITH TIME ZONE
            )
            """
    return query


def _create_mock2_sql_data() -> pd.DataFrame:
    """
    Create a Mock2 OHLCV dataframe.

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


# TODO(Dan): Consider joining `Mock1SqlRealTimeImClient` and `MockSqlRealTimeImClient`.
class MockSqlRealTimeImClient(icdc.SqlRealTimeImClient):
    """
    Vendor-agnostic client to be used in tests.
    """

    def __init__(
        self,
        db_connection: hsql.DbConnection,
        table_name: str,
        *,
        resample_1min: bool = False,
    ):
        vendor = "mock"
        super().__init__(
            vendor,
            db_connection,
            table_name,
            resample_1min=resample_1min,
        )

    @staticmethod
    def should_be_online() -> bool:
        """
        The real-time system for Talos should always be online.
        """
        return True


def get_mock_realtime_client(
    connection: hsql.DbConnection, *, resample_1min: bool = False
) -> Mock1SqlRealTimeImClient:
    """
    Set up a real time Mock2 SQL client.

    - Creates a Mock2 table
    - Uploads mock2 data
    - Creates a client connected to the given DB
    """
    # Create example table.
    table_name = "mock2_marketdata"
    query = _get_mock2_create_table_query()
    connection.cursor().execute(query)
    num_rows = hsqlimpl.get_num_rows(connection, table_name)
    if num_rows == 0:
        # Create a data example and upload to local DB.
        data = _create_mock2_sql_data()
        hsql.copy_rows_with_copy_from(connection, data, table_name)
    # Initialize a client connected to the local DB.
    im_client = MockSqlRealTimeImClient(
        connection, table_name, resample_1min=resample_1min
    )
    return im_client
