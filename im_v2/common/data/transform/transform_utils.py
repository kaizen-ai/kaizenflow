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
    # For clarify add +1 so the levels start from 1.
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
