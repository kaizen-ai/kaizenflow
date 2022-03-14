"""
Implement common transform operations.

Import as:

import im_v2.common.data.transform.transform_utils as imvcdttrut
"""

import logging

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
    if pd.api.types.is_integer_dtype(datetime_col_name):
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