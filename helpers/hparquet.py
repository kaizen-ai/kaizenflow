"""
Import as:

import helpers.hparquet as hparque
"""

import logging
from typing import Any, List, Optional, Tuple, Union

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hintrospection as hintros
import helpers.hio as hio
import helpers.hsystem as hsystem
import helpers.htimer as htimer

_LOG = logging.getLogger(__name__)


def from_parquet(
    file_name: str,
    *,
    columns: Optional[List[str]] = None,
    filters: Optional[List[Any]] = None,
    log_level: int = logging.DEBUG,
) -> pd.DataFrame:
    """
    Load a dataframe from a Parquet file.

    The difference with `pd.read_pq` is that here we use Parquet
    Dataset.
    """
    hdbg.dassert_isinstance(file_name, str)
    # hdbg.dassert_file_extension(file_name, ["pq", "parquet"])
    # Load data.
    with htimer.TimedScope(
        logging.DEBUG, f"# Reading Parquet file '{file_name}'"
    ) as ts:
        filesystem = None
        # TODO(gp): Generalize for S3.
        hdbg.dassert_exists(file_name)
        dataset = pq.ParquetDataset(
            file_name,
            filesystem=filesystem,
            filters=filters,
            use_legacy_dataset=False,
        )
        # To read also the index we need to use `read_pandas()`, instead of
        # `read_table()`.
        # See https://arrow.apache.org/docs/python/parquet.html#reading-and-writing-single-files.
        table = dataset.read_pandas(columns=columns)
        df = table.to_pandas()
    # Report stats.
    file_size = hsystem.du(file_name, human_format=True)
    _LOG.log(
        log_level,
        "Loaded '%s' (size=%s, time=%.1fs)",
        file_name,
        file_size,
        ts.elapsed_time,
    )
    # Report stats about the df.
    _LOG.debug("df.shape=%s", str(df.shape))
    mem = df.memory_usage().sum()
    _LOG.debug("df.memory_usage=%s", hintros.format_size(mem))
    return df


# TODO(gp): @Nikola allow to read / write from S3 passing aws_profile like done
#  in the rest of the code.
def to_parquet(
    df: pd.DataFrame,
    file_name: str,
    *,
    log_level: int = logging.DEBUG,
) -> None:
    """
    Save a dataframe as Parquet.
    """
    hdbg.dassert_isinstance(df, pd.DataFrame)
    hdbg.dassert_isinstance(file_name, str)
    hdbg.dassert_file_extension(file_name, ["pq", "parquet"])
    #
    hio.create_enclosing_dir(file_name, incremental=True)
    _LOG.debug("df.shape=%s", str(df.shape))
    mem = df.memory_usage().sum()
    _LOG.debug("df.memory_usage=%s", hintros.format_size(mem))
    # Save data.
    with htimer.TimedScope(
        logging.DEBUG, f"# Writing Parquet file '{file_name}'"
    ) as ts:
        table = pa.Table.from_pandas(df)
        pq.write_table(table, file_name)
    # Report stats.
    file_size = hsystem.du(file_name, human_format=True)
    _LOG.log(
        log_level,
        "Saved '%s' (size=%s, time=%.1fs)",
        file_name,
        file_size,
        ts.elapsed_time,
    )


# #############################################################################


def get_partition_columns(partition_mode: str) -> List[str]:
    """
    Get partition columns like year, month, day depending on partition mode.

    :param partition_mode:
        - "by_date": extract the date from the index
            - E.g., an index like `2022-01-10 14:00:00+00:00` is transform to a
              column `20220110`
        - "by_year_month_day": split the index in year, month, day columns
        - "by_year_month": split by year and month
        - "by_year_week": split by year and week of the year
        - "by_year": split by year
    :return: list of partitioning columns
    """
    if partition_mode == "by_date":
        partition_columns = ["date"]
    elif partition_mode == "by_year_month_day":
        partition_columns = ["year", "month", "date"]
    elif partition_mode == "by_year_month":
        partition_columns = ["year", "month"]
    elif partition_mode == "by_year_week":
        partition_columns = ["year", "weekofyear"]
    elif partition_mode == "by_year":
        partition_columns = ["year"]
    elif partition_mode == "by_month":
        partition_columns = ["month"]
    else:
        raise ValueError(f"Invalid partition_mode='{partition_mode}'")
    return partition_columns


PARQUET_AND_FILTER = List[List[Tuple[str, str, Union[int, List[int]]]]]


def get_parquet_filters_from_timestamp_interval(
    partition_mode: str,
    start_ts: Optional[pd.Timestamp],
    end_ts: Optional[pd.Timestamp],
) -> PARQUET_AND_FILTER:
    """
    Convert a constraint on a timestamp [start_timestamp, end_timestamp] into a
    Parquet filters expression, based on the passed partitioning/tiling
    criteria.

    :param partition_mode: mode to control filtering of parquet datasets
        that were previously saved in the same mode
    :param start_ts: start of the interval
    :param end_ts: end of the interval
    :return: list of AND(conjunction) predicates that are expressed
        in DNF(disjunctive normal form)
    """
    # Check timestamp interval.
    left_close = True
    right_close = True
    hdateti.dassert_is_valid_interval(
        start_ts, end_ts, left_close=left_close, right_close=right_close
    )
    # Obtain partition columns.
    partition_columns = get_partition_columns(partition_mode)
    # The data is stored by week, so we need to convert the timestamps into
    # weeks and then trim the excess.
    and_filters = []
    for timestamp, condition in [(start_ts, ">="), (end_ts, "<=")]:
        if timestamp is not None:
            # Add filter for each partition column.
            for column_name in partition_columns:
                # TODO(gp): Use weekofyear = start_ts.isocalendar().week
                # weekofyear = timestamp.week
                # and_condition = ("weekofyear", condition, weekofyear)
                time_part = getattr(timestamp, column_name)
                and_condition = (column_name, condition, time_part)
                and_filters.append(and_condition)
                _LOG.debug("added condition %s", str(and_condition))
    filters = [and_filters]
    _LOG.debug("filters=%s", str(filters))
    return filters
