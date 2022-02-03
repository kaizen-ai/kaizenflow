"""
Import as:

import helpers.hparquet as hparque
"""

import logging
from typing import Any, List, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

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

    The difference with `pd.read_pq` is that here we use Parquet Dataset.
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


# ###########################

ParquetFilter = List[Tuple[Tuple[str, str, str]]]

#def dassert_is_valid_parquet_filter(filters: ParquetFilter) -> None:


# TODO(gp): Should we extend it to left_close, right_close?
def get_parquet_filters_from_timestamp_interval(
        partitioning_mode: str,
        start_timestamp: Optional[pd.Timestamp],
        end_timestamp: Optional[pd.Timestamp]):
    """
    Convert a constraint on a timestamp [start_timestamp, end_timestamp]
    into a Parquet filters expression, based on the passed partitioning / tiling
    criteria.
    """
    hdateti.dassert_is_valid_interval(start_timestamp, end_timestamp)
    # Parquet `filters` is an OR of AND conditions. See
    # https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetDataset.html
    # The data is stored by week so we need to convert the timestamps into
    # weeks and then trim the excess.
    # Compute the start_date.
    if start_ts is not None:
        # TODO(gp): Use weekofyear = start_ts.isocalendar().week
        # weekofyear = start_ts.week
        # and_condition = ("weekofyear", ">=", weekofyear)
        month = start_ts.month
        and_condition = ("month", ">=", month)
        and_filters.append(and_condition)
        #
        year = start_ts.year
        and_condition = ("year", ">=", year)
        and_filters.append(and_condition)
    # Compute the end_date.
    hdateti.dassert_is_valid_timestamp(end_ts)
    if end_ts is not None:
        self._dassert_is_valid_timestamp(end_ts)
        # weekofyear = end_ts.week
        # and_condition = ("weekofyear", "<=", weekofyear)
        # and_filters.append(and_condition)
        month = end_ts.month
        and_condition = ("month", "<=", month)
        and_filters.append(and_condition)
        #
        year = end_ts.year
        and_condition = ("year", "<=", year)
        and_filters.append(and_condition)
    filters = [and_filters]
    _LOG.debug("filters=%s", str(filters))
