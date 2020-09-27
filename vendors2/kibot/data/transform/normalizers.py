import logging
from typing import Callable

import pandas as pd

import helpers.dbg as dbg
import vendors2.kibot.data.types as types

_LOG = logging.getLogger(__name__)


# TODO(gp): Call the column datetime_ET suffix.
def _normalize_1_min(df: pd.DataFrame) -> pd.DataFrame:
    """Convert a df with 1 min Kibot data into our internal format.

    - Combine the first two columns into a datetime index
    - Add column names
    - Check for monotonic index

    :param df: kibot raw dataframe as it is in .csv.gz files
    :return: a dataframe with `datetime` index and `open`, `high`,
        `low`, `close`, `vol` columns. If the input dataframe
        has only one column, the column name will be transformed to
        string format.
    """
    # There are cases in which the dataframes consist of only one column,
    # with the first row containing a `405 Data Not Found` string, and
    # the second one containing `No data found for the specified period
    # for BTSQ14.`
    if df.shape[1] > 1:
        # According to Kibot the columns are:
        # Date,Time,Open,High,Low,Close,Volume
        # Convert date and time into a datetime.
        df[0] = pd.to_datetime(df[0] + " " + df[1], format="%m/%d/%Y %H:%M")
        df.drop(columns=[1], inplace=True)
        # Rename columns.
        df.columns = "datetime open high low close vol".split()
        df.set_index("datetime", drop=True, inplace=True)
    else:
        df.columns = df.columns.astype(str)
        _LOG.warning("The dataframe has only one column:\n%s", df)
    dbg.dassert(df.index.is_monotonic_increasing)
    dbg.dassert(df.index.is_unique)
    return df


def _normalize_daily(df: pd.DataFrame) -> pd.DataFrame:
    """Convert a df with daily Kibot data into our internal format.

    - Convert the first column to datetime and set is as index
    - Add column names
    - Check for monotonic index

    :param df: kibot raw dataframe as it is in .csv.gz files
    :return: a dataframe with `datetime` index and `open`, `high`,
        `low`, `close`, `vol` columns.
    """
    # Convert date and time into a datetime.
    df[0] = pd.to_datetime(df[0], format="%m/%d/%Y")
    # Rename columns.
    df.columns = "datetime open high low close vol".split()
    df.set_index("datetime", drop=True, inplace=True)
    # TODO(gp): Turn date into datetime using EOD timestamp. Check on Kibot.
    dbg.dassert(df.index.is_monotonic_increasing)
    dbg.dassert(df.index.is_unique)
    return df


def get_normalizer(frequency: types.Frequency) -> Callable:
    """Choose a normalizer function based on a dataset name.

    :param dataset: dataset name
    :return: a function that can process a dataframe
    :raises AssertionError: if a frequency is not supported in 'Mapping'.
    """
    MAPPING = {
        types.Frequency.Daily: _normalize_daily,
        types.Frequency.Minutely: _normalize_1_min,
    }

    if frequency not in MAPPING:
        dbg.dfatal("Support for frequency '%s' not implemented yet", frequency)

    return MAPPING[frequency]
