"""
Import as:

import helpers.hparquet as hparque
"""

import logging
from typing import Any, List, Optional, Tuple

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

ParquetAndFilter = List[Tuple[str, str, Any]]
ParquetOrAndFilter = List[ParquetAndFilter]


def get_parquet_filters_from_timestamp_interval(
    partition_mode: str,
    start_timestamp: Optional[pd.Timestamp],
    end_timestamp: Optional[pd.Timestamp],
) -> ParquetOrAndFilter:
    """
    Convert a constraint on a timestamp [start_timestamp, end_timestamp] into a
    Parquet filters expression, based on the passed partitioning/tiling
    criteria.

    :param partition_mode: mode to control filtering of parquet datasets
        that were previously saved in the same mode
    :param start_timestamp: start of the interval
    :param end_timestamp: end of the interval
    :return: list of AND(conjunction) predicates that are expressed
        in DNF(disjunctive normal form)
    """
    # Check timestamp interval.
    left_close = True
    right_close = True
    hdateti.dassert_is_valid_interval(
        start_timestamp,
        end_timestamp,
        left_close=left_close,
        right_close=right_close,
    )
    # Use hardwired start and end date to represent the start_timestamp / end_timestamp = None.
    # This is not very elegant, but it simplifies the code
    min_date = pd.Timestamp("2001-01-01 00:00:00+00:00")
    max_date = pd.Timestamp("2100-01-01 00:00:00+00:00")
    if start_timestamp is None:
        start_timestamp = min_date
    if end_timestamp is None:
        end_timestamp = max_date
    filters = []
    # Partition by year and month.
    if partition_mode == "by_year_month":
        # Include last month in interval.
        end_timestamp = end_timestamp + pd.DateOffset(months=1)
        # Get all months in interval.
        dates = pd.date_range(start_timestamp, end_timestamp, freq="M")
        for date in dates:
            year = date.year
            month = date.month
            and_filter = [("year", "=", year), ("month", "=", month)]
            filters.append(and_filter)
            _LOG.debug("Adding AND filter %s", str(and_filter))
    # TODO(Nikola): Partition by week.
    #   week = start_ts.isocalendar()[1]
    #   https://docs.python.org/3/library/datetime.html#datetime.date.isocalendar
    _LOG.debug("filters=%s", str(filters))
    return filters
