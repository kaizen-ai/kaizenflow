"""
Import as:

import helpers.hpandas as hpandas
"""

from typing import Any, Optional, Union

import pandas as pd

import helpers.dbg as dbg


def dassert_index_is_datetime(
    df: pd.DataFrame, msg: Optional[str] = None, *args: Any
) -> None:
    """
    Ensure that the dataframe has an index containing datetimes.
    """
    import pandas as pd

    # TODO(gp): Add support also for series.
    dbg.dassert_isinstance(df, pd.DataFrame, msg, *args)
    dbg.dassert_isinstance(df.index, pd.DatetimeIndex, msg, *args)


def dassert_strictly_increasing_index(
    obj: Union[pd.Index, pd.DataFrame, pd.Series],
    msg: Optional[str] = None,
    *args: Any,
) -> None:
    """
    Ensure that the dataframe has a strictly increasing index.
    """
    import pandas as pd

    if isinstance(obj, pd.Index):
        index = obj
    else:
        index = obj.index
    # TODO(gp): Understand why mypy reports:
    #   error: "dassert" gets multiple values for keyword argument "msg"
    dbg.dassert(index.is_monotonic_increasing, msg=msg, *args)  # type: ignore
    dbg.dassert(index.is_unique, msg=msg, *args)  # type: ignore


# TODO(gp): Factor out common code related to extracting the index from several
#  pandas data structures.
# TODO(gp): Not sure it's used or useful?
def dassert_monotonic_index(
    obj: Union[pd.Index, pd.DataFrame, pd.Series],
    msg: Optional[str] = None,
    *args: Any,
) -> None:
    """
    Ensure that the dataframe has a strictly increasing or decreasing index.
    """
    # For some reason importing pandas is slow and we don't want to pay this
    # start up cost unless we have to.
    import pandas as pd

    if isinstance(obj, pd.Index):
        index = obj
    else:
        index = obj.index
    # TODO(gp): Understand why mypy reports:
    #   error: "dassert" gets multiple values for keyword argument "msg"
    cond = index.is_monotonic_increasing or index.is_monotonic_decreasing
    dbg.dassert(cond, msg=msg, *args)  # type: ignore
    dbg.dassert(index.is_unique, msg=msg, *args)  # type: ignore


# #############################################################################
# Parquet
# #############################################################################

def to_parquet(df: pd.DataFrame, file_name: str) -> None:
    dbg.dassert_is_instance(df, pd.DataFrame)
    dbg.dassert_type_is(file_name, str)
    dbg.dassert(file_name.endswith(".pq"), "Invalid file_name='%s'", file_name)
    #
    hio.create_enclosing_dir(file_name, incremental=True)
    _LOG.log(log_level, "df.shape=%s", str(df.shape))
    mem = df.memory_usage().sum()
    import helpers.introspection as hintro
    _LOG.log(log_level, "df.memory_usage=%s", hintro.format_size(mem))
    #
    dtmr = htimer.dtimer_start(log_level, "To parquet '%s'" % file_name)
    import pyarrow as pa
    import pyarrow.parquet as pq
    table = pa.Table.from_pandas(df)
    pq.write_table(table, file_name)
    #
    _, elapsed_time = htimer.dtimer_stop(dtmr)
    size_mb = hintro.format_size(os.path.getsize(file_name))
    # TODO(gp): Use hintro.format_size().
    if verbose:
        _LOG.info(
            "Saved '%s' (size=%.2f Mb, time=%.1fs)",
            file_name,
            size_mb,
            elapsed_time,
        )