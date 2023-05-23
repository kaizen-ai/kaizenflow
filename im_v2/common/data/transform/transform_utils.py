"""
Implement common transform operations.

Import as:

import im_v2.common.data.transform.transform_utils as imvcdttrut
"""

import logging
from typing import Dict, List

import numpy as np
import pandas as pd

import core.finance.resampling as cfinresa
import data_schema.dataset_schema_utils as dsdascut
import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hpandas as hpandas
import helpers.htimer as htimer

_LOG = logging.getLogger(__name__)

BID_ASK_COLS = ["bid_price", "bid_size", "ask_price", "ask_size"]
NUMBER_LEVELS_OF_ORDER_BOOK = 10


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
    :param expected_columns: columns that will be present in new re-indexed dataframe
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
    :return DataFrame with unfinished bars removed
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


def _transform_bid_ask_websocket_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform bid/ask raw DataFrame to DataFrame representation suitable for
    database insertion.

    :param df: DataFrame formed from raw bid/ask dict data.
    :return transformed DataFrame
    """
    df = df.explode(["asks", "bids"])
    df[["bid_price", "bid_size"]] = pd.DataFrame(
        df["bids"].to_list(), index=df.index
    )
    df[["ask_price", "ask_size"]] = pd.DataFrame(
        df["asks"].to_list(), index=df.index
    )
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
    :return transformed DataFrame
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
    #  TODO(Juraj): this holds only for binance data.
    df = df[
        pd.to_datetime(df["end_download_timestamp"]).map(
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
    :return transformed DataFrame
    """
    # Explode data column to get individual trades.
    df = df.explode("data", ignore_index=True)
    # Extract columns from data column.
    df = df.join(pd.json_normalize(df.pop("data")))
    return df[
        [
            "timestamp",
            "side",
            "price",
            "amount",
            "currency_pair",
            "end_download_timestamp",
        ]
    ]


def transform_raw_websocket_data(
    raw_data: List[Dict], data_type: str, exchange_id: str
) -> pd.DataFrame:
    """
    Transform list of raw websocket data into a DataFrame with columns
    compliant with the database representation.

    :param data: data to be transformed
    :param data_type: type of data, e.g. OHLCV
    :param exchange_id: ID of the exchange where the data come from
    :return database compliant DataFrame formed from raw data
    """
    df = pd.DataFrame(raw_data)
    if data_type == "ohlcv":
        df = _transform_ohlcv_websocket_dataframe(df)
    elif data_type == "bid_ask":
        df = _transform_bid_ask_websocket_dataframe(df)
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


def resample_bid_ask_data_from_1sec_to_1min(data: pd.DataFrame) -> pd.DataFrame:
    """
    Resample bid/ask data to 1 minute interval for single symbol.

    The method expects data in the following format:
            bid_size, bid_price, ask_size, ask_price
    timestamp 2022-11-16T00:00:01+00:00 5450, 13.50, 5200, 13.25

    The method expects data coming from a single exchange.

    :return data resampled to 1 minute.
    """
    # Set resample arguments according to our data invariant [a, b) and
    #  set label to 'b';
    try:
        hdbg.dassert_isinstance(data, pd.DataFrame)
        cols = ["bid_price", "bid_size", "ask_price", "ask_size"]
        hdbg.dassert_is_subset(cols, data.columns)
        for col in cols:
            hpandas.dassert_series_type_is(data[col], np.float64)
    except ValueError:
        _LOG.error("Input format is wrong")
    # First resample to 1 second.
    resampling_groups_1sec = [
        (
            {
                "bid_price": "bid_price",
                "bid_size": "bid_size",
                "ask_price": "ask_price",
                "ask_size": "ask_size",
            },
            "mean",
            {},
        ),
    ]
    resampling_kwargs_1sec = {
        "closed": "left",
        "label": "right",
    }
    data_1sec = cfinresa.resample_bars(
        data,
        "1s",
        resampling_groups_1sec,
        vwap_groups=[],
        resample_kwargs=resampling_kwargs_1sec,
    )
    # Forward fill.
    data_1sec = data_1sec.ffill()
    # Resample to 1 minute.
    resampling_groups_1min = [
        (
            {
                "bid_price": "bid_price.close",
                "bid_size": "bid_size.close",
                "ask_price": "ask_price.close",
                "ask_size": "ask_size.close",
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
            },
            "mean",
            {},
        ),
    ]
    data_1min = cfinresa.resample_bars(
        data_1sec,
        "1T",
        resampling_groups_1min,
        vwap_groups=[],
        resample_kwargs={},
    )
    return data_1min


def resample_multilevel_bid_ask_data_from_1sec_to_1min(
    data: pd.DataFrame,
) -> pd.DataFrame:
    """
    Resample multilevel bid/ask data to 1 minute interval for single symbol.

    The method expects data in the following format:
            exchange_id, bid_size_l1,bid_price_l1,ask_size_l1,ask_price_l1...
    timestamp 2022-11-16T00:00:01+00:00 binance, 5450, 13.50, 5200, 13.25...

    The method assumes NUMBER_LEVELS_OF_ORDER_BOOK levels of order book
    and a data coming from single exchange.

    :return DataFrame resampled to 1 minute.
    """
    all_levels_resampled = []
    for i in range(1, NUMBER_LEVELS_OF_ORDER_BOOK + 1):
        bid_ask_cols_level = list(map(lambda x: f"{x}_l{i}", BID_ASK_COLS))
        data_one_level = data[bid_ask_cols_level]
        # Canonize column name for resampling function.
        data_one_level.columns = BID_ASK_COLS
        data_one_level = resample_bid_ask_data_from_1sec_to_1min(data_one_level)
        # Uncanonize the column levels back.
        data_one_level = data_one_level.rename(columns=lambda x: f"level_{i}.{x}")
        all_levels_resampled.append(data_one_level)
    # Drop duplicate columns because a vetical concatenation follows.
    data_resampled = pd.concat(all_levels_resampled, axis=1)
    # Insert exchange_id column
    data_resampled["exchange_id"] = data["exchange_id"].iloc[0]
    return data_resampled


# TODO(Juraj, Vlad): add a unit test.
def transform_and_resample_rt_bid_ask_data(
    df_raw: pd.DataFrame,
) -> pd.DataFrame:
    """
    Transform raw bid/ask realtime data and resample to 1 min.

    The function expects raw bid/ask data from a single exchange
    sampled multiple times per second In the first step the raw data
    get resampled to 1 sec by applying mean(). The second step performs
    resampling to 1 min via sum for sizes and VWAP for prices.

    :param df_raw: real-time bid/ask data from a single exchange
    """
    # Remove duplicates, keep the latest record.
    df_raw = df_raw.sort_values("knowledge_timestamp", ascending=False)
    df_raw = df_raw.drop_duplicates(
        ["timestamp", "exchange_id", "currency_pair", "level"]
    )
    # Convert timestamp to pd.Timestamp and set as index before sending for resampling.
    df_raw["timestamp"] = df_raw["timestamp"].map(
        hdateti.convert_unix_epoch_to_timestamp
    )
    df_raw = df_raw.set_index("timestamp")
    dfs_resampled = []
    # Resample to 1 sec.
    #  to match resample_bid_ask_data_from_1sec_to_1min() interface.
    df_raw = (
        df_raw.groupby(["exchange_id", "currency_pair", "level"])
        .resample(rule="S", closed="left", label="left")
        .mean()
    )
    # Drop level column, it's already in index
    df_raw = df_raw.drop("level", axis=1)
    # Resample to 1 min.
    for group_cols, group in df_raw.groupby(
        ["exchange_id", "currency_pair", "level"]
    ):
        df_part = resample_bid_ask_data_from_1sec_to_1min(
            group.reset_index().set_index("timestamp")
        )
        # For DB we keep only mean columns.
        mean_cols = list(filter(lambda col: "mean" in col, df_part.columns))
        df_part = df_part[mean_cols]
        df_part.columns = list(
            map(lambda col: col.replace(".mean", ""), df_part.columns)
        )
        # Add the removed columns back.
        df_part[["exchange_id", "currency_pair", "level"]] = group_cols
        dfs_resampled.append(df_part)
    df_resampled = pd.concat(dfs_resampled)
    # Convert back to unix timestamp after resampling
    df_resampled = df_resampled.reset_index()
    df_resampled["timestamp"] = df_resampled["timestamp"].map(
        hdateti.convert_timestamp_to_unix_epoch
    )
    # This data is only reloaded from our DB so end_download_timestamp is None.
    df_resampled["end_download_timestamp"] = None
    # Round column values for readability.
    round_cols_dict = {
        col: 8 for col in ["bid_size", "bid_price", "ask_size", "ask_price"]
    }
    df_resampled = df_resampled.round(decimals=round_cols_dict)
    return df_resampled
