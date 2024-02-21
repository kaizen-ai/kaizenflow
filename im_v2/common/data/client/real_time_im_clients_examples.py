"""
Generate example data and initiate client for access to it.

Import as:

import im_v2.common.data.client.real_time_im_clients_examples as imvcdcrtimce
"""

import pandas as pd

import core.finance as cofinanc
import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hsql as hsql
import helpers.hsql_implementation as hsqlimpl
import im_v2.common.data.client as icdc

# #############################################################################
# TestSqlRealTimeImClient
# #############################################################################

_OHLCV_COLUMNS = [
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
]
# fmt: off
# pylint: disable=line-too-long
_OHLCV_MOCK_DATA = [
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

# TODO(Juraj): many unit test and mock classes create
# their own mechanisms for creating similar data. It could
# be better to create a single source of such data.
_BID_ASK_RESAMPLED_COLUMNS = [
    "id",
    "timestamp",
    "bid_size_open",
    "bid_size_close",
    "bid_size_min",
    "bid_size_max",
    "bid_size_mean",
    "bid_price_open",
    "bid_price_close",
    "bid_price_high",
    "bid_price_low",
    "bid_price_mean",
    "ask_size_open",
    "ask_size_close",
    "ask_size_min",
    "ask_size_max",
    "ask_size_mean",
    "ask_price_open",
    "ask_price_close",
    "ask_price_high",
    "ask_price_low",
    "ask_price_mean",
    "currency_pair",
    "exchange_id",
    "level",
    "end_download_timestamp",
    "knowledge_timestamp",
]
# fmt: off
# pylint: disable=line-too-long
_BID_ASK_RESAMPLED_DATA = [
    [0, 1650637800000, 5, 10, 15, 20, 30, 40, 50, 60, 30, 40, 50, 60, 30, 40, 50, 60, 30, 40, 50, 60, "ETH_USDT", "binance", 1, pd.Timestamp("2022-04-22"), pd.Timestamp("2022-04-22")],
    [1, 1650638400000, 11, 21, 31, 41, 51, 61, 71, 31, 41, 51, 61, 71, 31, 41, 51, 61, 71, 31, 41, 51, "BTC_USDT", "binance", 1, pd.Timestamp("2022-04-22"), pd.Timestamp("2022-04-22")],
    [2, 1650639600000, 11, 21, 31, 41, 51, 61, 71, 31, 41, 51, 61, 71, 31, 41, 51, 61, 71, 31, 41, 51, "ETH_USDT", "binance", 1, pd.Timestamp("2022-04-22"), pd.Timestamp("2022-04-22")],
    [3, 1650641400000, 11, 21, 31, 41, 51, 61, 71, 31, 41, 51, 61, 71, 31, 41, 51, 61, 71, 31, 41, 51, "BTC_USDT", "binance", 1, pd.Timestamp("2022-04-22"), pd.Timestamp("2022-04-22")],
    [4, 1650645000000, 11, 21, 31, 41, 51, 61, 71, 31, 41, 51, 61, 71, 31, 41, 51, 61, 71, 31, 41, 51, "ETH_USDT", "binance", 1, pd.Timestamp("2022-04-22"), pd.Timestamp("2022-04-22")],
    [5, 1650647400000, 11, 21, 31, 41, 51, 61, 71, 31, 41, 51, 61, 71, 31, 41, 51, 61, 71, 31, 41, 51, "BTC_USDT", "binance", 1, pd.Timestamp("2022-04-22"), pd.Timestamp("2022-04-22")],
]
# pylint: enable=line-too-long

# TODO(Grisha, Juraj): I don't see any usages, should we plan to deprecate?
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

#TODO(Grisha, Juraj): I don't see any usages, should we plan to deprecate?
def _create_mock1_sql_data() -> pd.DataFrame:
    """
    Generate a dataframe with price features and fixed currency_pair and
    exchange_id.

    This simulates contents of DBs with crypto data, e.g. from CCXT.

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
# MockSqlRealTimeImClient
# #############################################################################

# TODO(gp): Is this related to Mock2 system? If it is we should call it Mock2
#  and also rename some of the functions from `mock_` to `Mock2_`

#TODO(Grisha, Juraj): it's not ideal that this is decoupled from
# functions in im_v2.ccxt.db.utils we should consider fetching the query from there
# and str.replace() the name of the table in the query if that's preferred for the sake of tests.
def _get_mock2_create_table_query(table_name: str) -> str:
    """
    Get SQL query to create a test table.

    The table schema corresponds to the OHLCV data and is used for
    testing.

    :param table_name: determines what kind of table
    """
    if table_name == "ccxt_ohlcv_futures":
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
    elif table_name == "ccxt_bid_ask_futures_resampled_1min":
        query = """
            CREATE TABLE IF NOT EXISTS mock2_marketdata(
            id SERIAL PRIMARY KEY,
            timestamp BIGINT NOT NULL,
            "bid_size_open" NUMERIC,
            "bid_size_close" NUMERIC,
            "bid_size_min" NUMERIC,
            "bid_size_max" NUMERIC,
            "bid_size_mean" NUMERIC,
            "bid_price_open" NUMERIC,
            "bid_price_close" NUMERIC,
            "bid_price_high" NUMERIC,
            "bid_price_low" NUMERIC,
            "bid_price_mean" NUMERIC,
            "ask_size_open" NUMERIC,
            "ask_size_close" NUMERIC,
            "ask_size_min" NUMERIC,
            "ask_size_max" NUMERIC,
            "ask_size_mean" NUMERIC,
            "ask_price_open" NUMERIC,
            "ask_price_close" NUMERIC,
            "ask_price_high" NUMERIC,
            "ask_price_low" NUMERIC,
            "ask_price_mean" NUMERIC,
            currency_pair VARCHAR(255) NOT NULL,
            exchange_id VARCHAR(255) NOT NULL,
            level INTEGER NOT NULL,
            end_download_timestamp TIMESTAMP WITH TIME ZONE,
            knowledge_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
            """
    return query


def _create_mock2_sql_data(table_name: str) -> pd.DataFrame:
    """
    Create a Mock2 OHLCV dataframe.

    It recreates OHLCV data found in CCXT providers

    :param table_name: schema of which table to use to generate mock
        data.
    """
    if table_name == "ccxt_ohlcv_futures":
        test_data = pd.DataFrame(columns=_OHLCV_COLUMNS, data=_OHLCV_MOCK_DATA
        )
    elif table_name == "ccxt_bid_ask_futures_resampled_1min":
        test_data = pd.DataFrame(columns=_BID_ASK_RESAMPLED_COLUMNS, data=_BID_ASK_RESAMPLED_DATA
        )
    return test_data


class MockSqlRealTimeImClient(icdc.SqlRealTimeImClient):
    """
    Vendor-agnostic client to be used in tests.
    """

    def __init__(
        self,
        universe_version: str,
        db_connection: hsql.DbConnection,
        table_name: str,
        *,
        resample_1min: bool = False,
    ):
        vendor = "mock1"
        super().__init__(
            vendor,
            universe_version,
            db_connection,
            table_name,
            resample_1min=resample_1min,
        )

    @staticmethod
    def should_be_online() -> bool:
        """
        The real-time system should always be online.
        """
        return True


# TODO(gp): -> get_Mock2?
def get_mock_realtime_client(
    universe_version: str,
    connection: hsql.DbConnection,
    table_name: str,
    *,
    resample_1min: bool = False,
) -> MockSqlRealTimeImClient:
    """
    Set up a real time Mock2 SQL client.

    - Creates a Mock2 table
    - Uploads mock2 data
    - Creates a client connected to the given DB
    """
    hdbg.dassert_in(table_name, ["ccxt_ohlcv_futures", "ccxt_bid_ask_futures_resampled_1min"])
    # Create example table.
    query = _get_mock2_create_table_query(table_name)
    connection.cursor().execute(query)
    mock_table_name = "mock2_marketdata"
    num_rows = hsqlimpl.get_num_rows(connection, mock_table_name)
    if num_rows == 0:
        # Create a data example and upload to local DB.
        data = _create_mock2_sql_data(table_name)
        hsql.copy_rows_with_copy_from(connection, data, mock_table_name)
    # Initialize a client connected to the local DB.
    im_client = MockSqlRealTimeImClient(
        universe_version, connection, mock_table_name, resample_1min=resample_1min
    )
    return im_client
