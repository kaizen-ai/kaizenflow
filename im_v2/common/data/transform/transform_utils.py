"""
Implement common transform operations.

Import as:

import im_v2.common.data.transform.transform_utils as imvcdttrut
"""

import logging
from typing import Dict, List

import pandas as pd

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.htimer as htimer
import core.finance.resampling as cfinresa

_LOG = logging.getLogger(__name__)


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
    else:
        raise ValueError(
            "Transformation of data type: %s is not supported", data_type
        )
    df = df.drop_duplicates()
    df["exchange_id"] = exchange_id
    return df


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
    df = df[
        [
            "currency_pair",
            "timestamp",
            "bid_price",
            "bid_size",
            "ask_price",
            "ask_size",
            "end_download_timestamp",
        ]
    ]
    df = df.drop_duplicates()
    # For clarity, add +1 so the levels start from 1.
    df["level"] = df.groupby(groupby_cols).cumcount().add(1)
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
    data: pd.DataFrame, mode: str = "VWAP"
) -> pd.DataFrame:
    """
    Resample bid/ask data to 1 minute interval.

    :param mode: designate strategy to use, i.e. volume-weighted average
        (VWAP) or time-weighted average price (TWAP)
    """
    resample_kwargs = {
        "rule": "T",
        "closed": None,
        "label": None,
    }
    if mode == "VWAP":
        bid_price = calculate_vwap(
            data, "bid_price", "bid_size", **resample_kwargs
        )
        ask_price = calculate_vwap(
            data, "ask_price", "ask_size", **resample_kwargs
        )
        bid_ask_price_df = pd.concat([bid_price, ask_price], axis=1)
    elif mode == "TWAP":
        bid_ask_price_df = (
            data[["bid_size", "ask_size"]]
            .groupby(pd.Grouper(freq=resample_kwargs["rule"]))
            .mean()
        )
    else:
        raise ValueError(f"Invalid mode='{mode}'")
    df = cfinresa.resample(data, **resample_kwargs).agg(
        {
            "bid_size": "sum",
            "ask_size": "sum",
            "exchange_id": "last",
        }
    )
    df.insert(0, "bid_price", bid_ask_price_df["bid_size"])
    df.insert(2, "ask_price", bid_ask_price_df["ask_size"])
    return df

#TODO(Juraj): extend to support deeper levels of order book.
def transform_and_resample_bid_ask_rt_data(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Transform raw bid/ask realtime data and resample to 1-min.
    """
    # Currently only top of the book is supported.
    df_raw = df_raw[df_raw["level"] == 1]
    # Convert timestamp to pd.Timestamp and set as index before sending for resampling.
    df_raw["timestamp"] = df_raw["timestamp"].map(hdateti.convert_unix_epoch_to_timestamp)
    df_raw = df_raw.set_index("timestamp")
    dfs_resampled = []
    for currency_pair in df_raw["currency_pair"].unique():
        df_part = df_raw[df_raw["currency_pair"] == currency_pair]
        df_part = resample_bid_ask_data(df_part)
        df_part["currency_pair"] = currency_pair
        dfs_resampled.append(df_part)
    df_resampled = pd.concat(dfs_resampled)
    # Convert back to unix timestamp after resampling
    df_resampled = df_resampled.reset_index()
    df_resampled["timestamp"] = df_resampled["timestamp"].map(hdateti.convert_timestamp_to_unix_epoch)
    # This data is only reloaded so end_download_timestamp is None.
    df_resampled["end_download_timestamp"] = None
    # Only top of the book currently supported
    df_resampled["level"] = 1
    # Apply rounding functions so the data is more readable.
    round_cols_dict = { col: 3 for col in ["bid_size", "bid_price", "ask_size", "ask_price"]}
    df_resampled = df_resampled.round(decimals=round_cols_dict)
    return df_resampled