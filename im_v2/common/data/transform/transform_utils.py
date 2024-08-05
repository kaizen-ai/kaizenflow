"""
Implement common transform operations.

Import as:

import im_v2.common.data.transform.transform_utils as imvcdttrut
"""

import logging
from typing import Dict, List

import numpy as np
import pandas as pd

import core.finance.bid_ask as cfibiask
import core.finance.resampling as cfinresa
import data_schema.dataset_schema_utils as dsdascut
import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hpandas as hpandas
import helpers.htimer as htimer

_LOG = logging.getLogger(__name__)

BID_ASK_COLS = ["bid_price", "bid_size", "ask_price", "ask_size"]
# NUMBER_LEVELS_OF_ORDER_BOOK = 10

# Forward fill is done to resample on grid, this defines the extent of forward fill
FFILL_LIMIT = 601
# Currently working at 100 millisecond resolution.
TIME_RESOLUTION_MS = 100


def get_vendor_epoch_unit(vendor: str, data_type: str) -> str:
    """
    Get the unit of unix epoch for a vendor.

    :param vendor: vendor name
    :param data_type: data type
    :return: unit of unix epoch
    """
    # Check whether the vendor and data type are allowed.
    schema_allowed_values = dsdascut.get_dataset_schema()["allowed_values"]
    vendors_from_schema = schema_allowed_values["vendor"]
    data_types_from_schema = schema_allowed_values["data_type"]
    hdbg.dassert_in(vendor, vendors_from_schema)
    hdbg.dassert_in(data_type, data_types_from_schema)
    # Get the unit of unix epoch based on the vendor and data type.
    unit = "ms"
    if vendor == "crypto_chassis":
        unit = "s"
        if data_type == "trades":
            unit = "ms"
    return unit


# TODO(Juraj): add argument to pass custom callable to get current time.
def add_knowledge_timestamp_col(df: pd.DataFrame, tz: str) -> pd.DataFrame:
    """
    Add 'knowledge_timestamp' column to a DataFrame and set the value to a
    current time using helpers.hdatetime.get_current_time.

    :param df: DataFrame to modify
    :param tz: timezone to use
    :return: input DataFrame with an added knowledge_timestamp column
    """
    df["knowledge_timestamp"] = hdateti.get_current_time(tz)
    return df


def convert_timestamp_column(
    datetime_col_name: pd.Series,
    unit: str = "ms",
) -> pd.Series:
    """
    Convert datetime as string or int into a timestamp.

    :param datetime_col_name: series containing datetime as str or int
    :param unit: the unit of unix epoch
    :return: series containing datetime as `pd.Timestamp`
    """
    if pd.to_numeric(
        datetime_col_name, errors="coerce"
    ).notnull().all() and not pd.api.types.is_float_dtype(datetime_col_name):
        # Check whether the column is numeric but not float typed.
        # Convert unix epoch into timestamp.
        kwargs = {"unit": unit}
        converted_datetime_col = datetime_col_name.apply(
            hdateti.convert_unix_epoch_to_timestamp, **kwargs
        )
    elif pd.api.types.is_string_dtype(datetime_col_name):
        # Convert string into timestamp.
        converted_datetime_col = hdateti.to_generalized_datetime(
            datetime_col_name
        )
    else:
        raise ValueError(
            "Incorrect data format. Datetime column should be of int or str dtype"
        )
    return converted_datetime_col


def reindex_on_datetime(
    df: pd.DataFrame, datetime_col_name: str, unit: str = "ms"
) -> pd.DataFrame:
    """
    Set datetime index to the dataframe.

    :param df: dataframe without datetime index
    :param datetime_col_name: name of the column containing time info
    :param unit: the unit of unix epoch
    :return: dataframe with datetime index
    """
    hdbg.dassert_in(datetime_col_name, df.columns)
    hdbg.dassert_ne(
        df.index.inferred_type, "datetime64", "Datetime index already exists"
    )
    with htimer.TimedScope(logging.DEBUG, "# reindex_on_datetime"):
        datetime_col_name = df[datetime_col_name]
        # Convert original datetime column into `pd.Timestamp`.
        datetime_idx = convert_timestamp_column(datetime_col_name, unit=unit)
        df = df.set_index(datetime_idx)
    return df


def reindex_on_custom_columns(
    df: pd.DataFrame, index_columns: List[str], expected_columns: List[str]
) -> pd.DataFrame:
    """
    Reindex dataframe on provided index columns.

    :param df: original dataframe
    :param index_columns: columns that will be used to create new index
    :param expected_columns: columns that will be present in new re-
        indexed dataframe
    :return: re-indexed dataframe
    """
    hdbg.dassert_is_subset(expected_columns, df.columns)
    data_reindex = df.loc[:, expected_columns]
    data_reindex = data_reindex.drop_duplicates()
    # Remove index name, so there is no conflict with column names.
    data_reindex.index.name = None
    data_reindex = data_reindex.sort_values(by=index_columns)
    data_reindex = data_reindex.set_index(index_columns)
    return data_reindex


def remove_unfinished_ohlcv_bars(data: pd.DataFrame) -> pd.DataFrame:
    """
    Remove unfinished OHLCV bars, i.e. bars for which it holds that.

    end_download_timestamp - timestamp < 60s.

    Some exchanges, e.g. binance, label a candle representing
    1/1/2022 10:59 - 1/1/2022 11:00 with timestamp 1/1/2022 10:59
    If the bar has been downloaded less than a minute after the
    candle start. the candle will contain unfinished data.

    :param data: DataFrame to filter unfinished bars from
    :return: DataFrame with unfinished bars removed
    """
    hdbg.dassert_is_subset(
        ["timestamp", "end_download_timestamp"], list(data.columns)
    )
    time_diff = (
        pd.to_datetime(data["end_download_timestamp"]).map(
            hdateti.convert_timestamp_to_unix_epoch
        )
        - data["timestamp"]
    )
    data = data.loc[time_diff >= 60000]
    return data


# #############################################################################
# Transform utils for raw websocket data
# #############################################################################


def _transform_bid_ask_websocket_dataframe(
    df: pd.DataFrame, exchange_id: str, max_num_levels: int
) -> pd.DataFrame:
    """
    Transform bid/ask raw DataFrame to DataFrame representation suitable for
    database insertion.

    :param df: DataFrame formed from raw bid/ask dict data.
    :param max_num_levels: filter bid ask data on level <=
        max_num_levels
    :return: transformed DataFrame
    """
    df = df.explode(["asks", "bids"])
    if (
        exchange_id == "binance"
        or exchange_id == "okx"
        or exchange_id == "kraken"
    ):
        df[["bid_price", "bid_size"]] = pd.DataFrame(
            df["bids"].to_list(), index=df.index
        )
        df[["ask_price", "ask_size"]] = pd.DataFrame(
            df["asks"].to_list(), index=df.index
        )
    elif exchange_id == "cryptocom":
        df[["bid_price", "bid_size", "bid_quantity"]] = pd.DataFrame(
            df["bids"].to_list(), index=df.index
        )
        df[["ask_price", "ask_size", "ask_quantity"]] = pd.DataFrame(
            df["asks"].to_list(), index=df.index
        )
    else:
        raise ValueError(f"Invalid exchange_id='{exchange_id}'")
    df["currency_pair"] = df["symbol"].str.replace("/", "_")
    groupby_cols = ["currency_pair", "timestamp"]
    # Drop duplicates before computing level column.
    non_metadata_columns = [
        "currency_pair",
        "timestamp",
        "bid_price",
        "bid_size",
        "ask_price",
        "ask_size",
    ]
    df = df[non_metadata_columns + ["end_download_timestamp"]]
    # It can happen that the orderbook did not change between iteration
    #  in this case we get duplicated data with different end_download_timestamp
    #  these can be safely dropped.
    df = df.drop_duplicates(non_metadata_columns)
    # For clarity, add +1 so the levels start from 1.
    df["level"] = df.groupby(groupby_cols).cumcount().add(1)
    _LOG.info(f"Filtering bid ask data until level {max_num_levels}")
    df = df[df["level"] <= max_num_levels]
    # Check for NaNs in the timestamp column and drop them.
    if df["timestamp"].isnull().any():
        _LOG.warning(
            "NaNs in timestamp column."
            " Number of rows before dropping NaNs: %s",
            len(df),
        )
        df = df.dropna(subset=["timestamp"])
        _LOG.warning("Number of rows after dropping NaNs: %s", len(df))
    return df


def _transform_ohlcv_websocket_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform bid/ask raw DataFrame to DataFrame representation suitable for
    database insertion.

    :param df: DataFrame formed from raw bid/ask dict data.
    :return: transformed DataFrame
    """
    df["currency_pair"] = df["currency_pair"].str.replace("/", "_")
    # Each message stores ohlcv candles as a list of lists.
    df = df.explode("ohlcv")
    df[["timestamp", "open", "high", "low", "close", "volume"]] = pd.DataFrame(
        df["ohlcv"].tolist(), index=df.index
    )
    # Remove bars which are certainly unfinished
    #  bars with end_download_timestamp which is not atleast
    #  a minute (60000 ms) after the timestamp are certainly unfinished.
    # Note: this holds only when exchange is using the opening, see
    # docs/datapull/ck.ccxt_exchange_timestamp_interpretation.reference.md
    # Note: format="ISO8601" is needed because of CmTask7859.
    df = df[
        pd.to_datetime(df["end_download_timestamp"], format="ISO8601").map(
            hdateti.convert_timestamp_to_unix_epoch
        )
        >= df["timestamp"] + 60000
    ]
    return df[
        [
            "currency_pair",
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "end_download_timestamp",
        ]
    ]


def transform_trades_websocket_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform trades raw DataFrame to DataFrame representation suitable for
    database insertion.

    :param df: DataFrame formed from raw trades dict data.
    :return: transformed DataFrame
    """
    # Explode data column to get individual trades.
    df = df.explode("data", ignore_index=True)
    # Extract columns from data column.
    df = df.join(pd.json_normalize(df.pop("data")))
    return df[
        [
            "id",
            "timestamp",
            "side",
            "price",
            "amount",
            "currency_pair",
            "end_download_timestamp",
        ]
    ]


def transform_raw_websocket_data(
    raw_data: List[Dict],
    data_type: str,
    exchange_id: str,
    *,
    max_num_levels: int = 10,
) -> pd.DataFrame:
    """
    Transform list of raw websocket data into a DataFrame with columns
    compliant with the database representation.

    :param data: data to be transformed
    :param data_type: type of data, e.g. OHLCV
    :param exchange_id: ID of the exchange where the data come from
    :param max_num_levels: Bid ask downloads minimum of 10 levels of
        data but with increasing universe it is impractical to dump all
        the levels. This flag when set will filter bid ask data till
        level <= max_num_levels
    :return: database compliant DataFrame formed from raw data
    """
    df = pd.DataFrame(raw_data)
    if data_type == "ohlcv" or data_type == "ohlcv_from_trades":
        df = _transform_ohlcv_websocket_dataframe(df)
    elif data_type == "bid_ask":
        df = _transform_bid_ask_websocket_dataframe(
            df, exchange_id, max_num_levels
        )
    elif data_type == "trades":
        df = transform_trades_websocket_dataframe(df)
    else:
        raise ValueError(
            f"Transformation of data type: {data_type} is not supported"
        )
    df = df.drop_duplicates()
    df["exchange_id"] = exchange_id
    return df


def transform_s3_to_db_format(data: pd.DataFrame) -> pd.DataFrame:
    """
    Transform data from S3 to DB format.

        Examples of data:
            Before:
    ```
                                   timestamp  bid_price_l1  bid_size_l1  ...  currency_pair  year  month
    timestamp                                                            ...
    2023-01-01 00:01:00+00:00  1672531260000      0.245553   16478619.0  ...       ADA_USDT  2023      1
    2023-01-01 00:02:00+00:00  1672531320000      0.245700   17432756.0  ...       ADA_USDT  2023      1
    ```
            After:
    ```
                timestamp exchange_id              knowledge_timestamp currency_pair  level  ...    bid_size  bid_price    ask_size  ask_price
    0       1672531260000     binance 2023-01-24 08:48:45.021468+00:00      ADA_USDT      1  ...  16478619.0   0.245553   5937479.0   0.245665
    1       1672531260000     binance 2023-01-24 08:48:45.021468+00:00      ADA_USDT      2  ...  20419132.0   0.245448  25896505.0   0.245754
    2       1672531260000     binance 2023-01-24 08:48:45.021468+00:00      ADA_USDT      3  ...  25726491.0   0.245354  21769380.0   0.245853
    ```

    :param data: dataframe from S3 to transform
    :return: transformed dataframe
    """
    data = data.set_index("timestamp", drop=True).reset_index()
    data = pd.wide_to_long(
        data,
        stubnames=["bid_size", "ask_size", "bid_price", "ask_price"],
        i=["timestamp", "exchange_id", "currency_pair", "knowledge_timestamp"],
        j="level",
        sep="_l",
    )
    data = data.reset_index().drop(columns=["year", "month"])
    return data


# #############################################################################
# Transform utils for resampling bid/ask data
# #############################################################################


def calculate_vwap(
    data: pd.Series, price_col: str, volume_col: str, **resample_kwargs
) -> pd.DataFrame:
    price = (
        data[price_col]
        .multiply(data[volume_col])
        .resample(**resample_kwargs)
        .agg({f"{volume_col}": "sum"})
    )
    size = data[volume_col].resample(**resample_kwargs).agg({volume_col: "sum"})
    calculated_price = price.divide(size)
    return calculated_price


def resample_bid_ask_data(
    data: pd.DataFrame,
    freq: str,
    *,
    time_resolution_in_ms: int = 200,
) -> pd.DataFrame:
    """
    Resample single level of bid/ask data to 1 minute interval for single
    symbol.

    The method expects data in the following format:

    ```
                                        bid_size, bid_price, ask_size, ask_price
    timestamp 2022-11-16T00:00:01+00:00     5450,     13.50,     5200,     13.25
    ```

    :param data: bid/ask price and size information for a single symbol, indexed by
        point-in-time
    :param freq: frequency at which we want to do resampling
    :return: data resampled to 1 minute bars
    """
    try:
        hdbg.dassert_isinstance(data, pd.DataFrame)
        cols = ["bid_price", "bid_size", "ask_price", "ask_size"]
        hdbg.dassert_set_eq(cols, data.columns, "")
        for col in cols:
            hpandas.dassert_series_type_is(data[col], np.float64)
    except ValueError:
        _LOG.error("Input format is wrong")
    # Compute point-in-time derived columns.
    bid_ask_midpoint = 0.5 * (data["ask_price"] + data["bid_price"])
    half_spread = 0.5 * (data["ask_price"] - data["bid_price"])
    log_size_imbalance = np.log(data["bid_size"]) - np.log(data["ask_size"])
    # Insert new columns.
    data["bid_ask_midpoint"] = bid_ask_midpoint
    data["half_spread"] = half_spread
    data["log_size_imbalance"] = log_size_imbalance
    # TODO(Juraj): There is a potential issue which will be analysed during
    #  CmTask4874, where we do not receive updated order book if it remained unchanged
    # for several sampling periods.
    # Forward fill.
    rule = str(int(time_resolution_in_ms / 2)) + "ms"
    # TODO(Paul): Consider parametrizing `limit`.
    data = cfinresa.resample(data, rule=rule).last().ffill(limit=FFILL_LIMIT)
    # Compute derived columns with time diff. We do this after resampling
    # everything on a uniform grid.
    bid_ask_midpoint_var = data["bid_ask_midpoint"].diff() ** 2
    bid_ask_midpoint_autocovar = data["bid_ask_midpoint"].diff() * data[
        "bid_ask_midpoint"
    ].diff().shift(1)
    log_size_imbalance_var = data["log_size_imbalance"] ** 2
    log_size_imbalance_autocovar = data["log_size_imbalance"] * data[
        "log_size_imbalance"
    ].shift(1)
    # Insert new columns.
    data["bid_ask_midpoint_var"] = bid_ask_midpoint_var
    data["bid_ask_midpoint_autocovar"] = bid_ask_midpoint_autocovar
    data["log_size_imbalance_var"] = log_size_imbalance_var
    data["log_size_imbalance_autocovar"] = log_size_imbalance_autocovar
    # Resample.
    resampling_groups = [
        (
            {
                "bid_price": "bid_price.open",
                "bid_size": "bid_size.open",
                "ask_price": "ask_price.open",
                "ask_size": "ask_size.open",
                "bid_ask_midpoint": "bid_ask_midpoint.open",
                "half_spread": "half_spread.open",
                "log_size_imbalance": "log_size_imbalance.open",
            },
            "first",
            {},
        ),
        (
            {
                "bid_price": "bid_price.close",
                "bid_size": "bid_size.close",
                "ask_price": "ask_price.close",
                "ask_size": "ask_size.close",
                "bid_ask_midpoint": "bid_ask_midpoint.close",
                "half_spread": "half_spread.close",
                "log_size_imbalance": "log_size_imbalance.close",
            },
            "last",
            {},
        ),
        (
            {
                "bid_price": "bid_price.high",
                "bid_size": "bid_size.max",
                "ask_price": "ask_price.high",
                "ask_size": "ask_size.max",
                "bid_ask_midpoint": "bid_ask_midpoint.max",
                "half_spread": "half_spread.max",
                "log_size_imbalance": "log_size_imbalance.max",
            },
            "max",
            {},
        ),
        (
            {
                "bid_price": "bid_price.low",
                "bid_size": "bid_size.min",
                "ask_price": "ask_price.low",
                "ask_size": "ask_size.min",
                "bid_ask_midpoint": "bid_ask_midpoint.min",
                "half_spread": "half_spread.min",
                "log_size_imbalance": "log_size_imbalance.min",
            },
            "min",
            {},
        ),
        (
            {
                "bid_price": "bid_price.mean",
                "bid_size": "bid_size.mean",
                "ask_price": "ask_price.mean",
                "ask_size": "ask_size.mean",
                "bid_ask_midpoint": "bid_ask_midpoint.mean",
                "half_spread": "half_spread.mean",
                "log_size_imbalance": "log_size_imbalance.mean",
            },
            "mean",
            {},
        ),
        (
            {
                "bid_ask_midpoint_var": f"bid_ask_midpoint_var.{rule}",
                "bid_ask_midpoint_autocovar": f"bid_ask_midpoint_autocovar.{rule}",
                "log_size_imbalance_var": f"log_size_imbalance_var.{rule}",
                "log_size_imbalance_autocovar": f"log_size_imbalance_autocovar.{rule}",
            },
            "sum",
            {},
        ),
    ]
    data = cfinresa.resample_bars(
        data,
        freq,
        resampling_groups,
        vwap_groups=[],
        resample_kwargs={},
    )
    return data


def resample_multilevel_bid_ask_data(
    data: pd.DataFrame,
    freq: str,
    *,
    number_levels_of_order_book: int = 10,
) -> pd.DataFrame:
    """
    Resample multilevel bid/ask data to 1 minute interval for single symbol.

    The method expects data in the following format:
    ```
                                        exchange_id, bid_size_l1, bid_price_l1, ask_size_l1, ask_price_l1...
    timestamp 2022-11-16T00:00:01+00:00     binance,        5450,        13.50,        5200,        13.25...
    ```

    The method assumes NUMBER_LEVELS_OF_ORDER_BOOK levels of order book
    and a data coming from single exchange.

    :param data: bid/ask data which is to be resampled
    :param freq: frequency at which the data will be resampled

    :return: DataFrame resampled to 1 minute.
    """
    all_levels_resampled = []
    for i in range(1, number_levels_of_order_book + 1):
        bid_ask_cols_level = list(map(lambda x: f"{x}_l{i}", BID_ASK_COLS))
        data_one_level = data[bid_ask_cols_level]
        # Canonize column name for resampling function.
        data_one_level.columns = BID_ASK_COLS
        data_one_level = resample_bid_ask_data(data_one_level, freq)
        # Uncanonize the column levels back.
        data_one_level = data_one_level.rename(columns=lambda x: f"level_{i}.{x}")
        all_levels_resampled.append(data_one_level)
    data_resampled = pd.concat(all_levels_resampled, axis=1)
    # Insert exchange_id column
    data_resampled["exchange_id"] = data["exchange_id"].iloc[0]
    return data_resampled


def resample_multisymbol_multilevel_bid_ask_data(
    data: pd.DataFrame,
    freq: str,
    *,
    number_levels_of_order_book: int = 10,
    currency_pairs: List[str] = None,
) -> pd.DataFrame:
    """
    Resample multilevel bid/ask data to `freq` interval.

    This function supports resampling DataFrame with multiple assets from
    a single exchange.

    The method expects data in the following format:

    ```
            exchange_id, currency_pair, bid_size_l1,bid_price_l1,ask_size_l1,ask_price_l1...
    timestamp 2022-11-16T00:00:01+00:00 binance, BTC_USDT, 5450, 13.50, 5200, 13.25...
    ```
    :param number_levels_of_order_book: top N levels to include in the resulting
    DataFrame.
    :return: DataFrame resampled to `freq`.
    """
    hdbg.dassert_eq(data["exchange_id"].nunique(), 1)
    data_resampled = []
    if not currency_pairs:
        currency_pairs = data["currency_pair"].unique()
    for currency_pair in currency_pairs:
        group = data[data["currency_pair"] == currency_pair]
        data_resampled_single = resample_multilevel_bid_ask_data(
            group,
            freq,
            number_levels_of_order_book=number_levels_of_order_book,
        )
        data_resampled_single["currency_pair"] = currency_pair
        data_resampled.append(data_resampled_single)
    data_resampled = pd.concat(data_resampled)
    return data_resampled


def transform_and_resample_rt_bid_ask_data(
    df_raw: pd.DataFrame,
    freq: str,
    *,
    currency_pairs: List[str] = None,
) -> pd.DataFrame:
    """
    Transform raw bid/ask realtime data and resample.

    Input data is assumed to be compatible with schema of `ccxt_bid_ask_futures_raw`
    also assumed to have data from single exchange.
    Currently only level 1 of the orderbook is returned in the resampled DataFrame.

    :param df_raw: real-time bid/ask data from a single exchange
    :param freq: frequency that want we to resample our data, e.g., "10S", "1T",

    :return: Data resampled to `freq` output column schema
    is compatible with `ccxt_bid_ask_futures_resampled_1min` table.

    Example:
        ---------------
        Input DataFrame (df_raw):
        -------------------------
             timestamp      currency_pair  level  bid_price  bid_size  ask_price  ask_size  id  end_download_timestamp  knowledge_timestamp exchange_id
        0  1622548800000      BTC_USDT      1      35000       1.2      35005       1.1   1           1622548801000        1622548802000     binance
        1  1622548805000      BTC_USDT      2      35010       1.5      35015       1.4   2           1622548806000        1622548807000     binance
        2  1622548810000      BTC_USDT      3      35020       1.8      35025       1.7   3           1622548811000        1622548812000     binance

        Output DataFrame after resampling to 1 second frequency ("1S"):
        ----------------------------------------------------------------
            timestamp        bid_price_open  bid_size_open  ask_price_open  ask_size_open  bid_ask_midpoint_open  half_spread_open  log_size_imbalance_open  bid_price_close  bid_size_close  ask_price_close  ask_size_close  bid_ask_midpoint_close  half_spread_close  log_size_imbalance_close  bid_price_high  bid_size_max  ask_price_high  ask_size_max  bid_ask_midpoint_max  half_spread_max  log_size_imbalance_max  bid_price_low  bid_size_min  ask_price_low  ask_size_min  bid_ask_midpoint_min  half_spread_min  log_size_imbalance_min  bid_price_mean  bid_size_mean  ask_price_mean  ask_size_mean  bid_ask_midpoint_mean  half_spread_mean  log_size_imbalance_mean  bid_ask_midpoint_var_100ms  bid_ask_midpoint_autocovar_100ms  log_size_imbalance_var_100ms  log_size_imbalance_autocovar_100ms exchange_id currency_pair  level
        0  1622548800000         35000.0          1.2           35005.0          1.1              35002.5               2.5               0.087011               35000.0           1.2            35005.0           1.1               35002.5                2.5                  0.087011             35000.0           1.2         35005.0           1.1            35002.5              2.5                0.087011            35000.0         1.2        3  5005.0         1.1             35002.5              2.5                0.087011            35000.0          1.2           35005.0            1.1             35002.5               2.5                 0.087011                    0.0                            0.0                         0.007571                             0.0                   binance      BTC_USDT       1
        ---------------
    """
    # TODO(Juraj): what's happening is here very similar to
    # `im_v2/common/data/transform/resample_daily_bid_ask_data.py`
    # we should unify.
    df_raw = df_raw.drop(
        ["id", "end_download_timestamp", "knowledge_timestamp"], axis=1
    )
    # TODO(Juraj): for simplicty only level 1 is used now.
    df_raw = df_raw[df_raw["level"] == 1]
    # TODO(Juraj): we are aware of duplicate data problem #7230 but for the
    # sake of resampled data availability we allow occasional duplicate with different values.
    duplicate_cols_subset = [
        "timestamp",
        "currency_pair",
        "level",
    ]
    # Ensure there are no duplicates, in this case
    #  using duplicates would compute the wrong resampled values.
    df_raw = df_raw.drop_duplicates(subset=duplicate_cols_subset)
    df_raw["timestamp"] = pd.to_datetime(df_raw["timestamp"], unit="ms")
    df_raw = df_raw.set_index("timestamp")
    df_raw_wide = cfibiask.transform_bid_ask_long_data_to_wide(
        df_raw, "timestamp"
    )
    df_resampled = resample_multisymbol_multilevel_bid_ask_data(
        df_raw_wide,
        freq,
        number_levels_of_order_book=1,
        currency_pairs=currency_pairs,
    )
    # Rename column to match DB table schema.
    # TODO(Juraj): the lambda will need to be more sophisticated for more levels.
    df_resampled.columns = list(
        map(
            lambda col: col.replace("level_1.", "").replace(".", "_"),
            df_resampled,
        )
    )
    # Resetting index is needed before inserting to RDS,
    # because the column is passed to the query.
    df_resampled = df_resampled.reset_index()
    # TODO(Juraj): librarize this, we tend to use .apply(hdateti.convert_timestamp_to_unix_epoch) but that's
    # slow.
    df_resampled["timestamp"] = (
        df_resampled["timestamp"] - pd.Timestamp("1970-01-01")
    ) // pd.Timedelta("1ms")
    # Add back level column because DB table is in long format.
    df_resampled["level"] = 1
    return df_resampled
