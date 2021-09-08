"""
Import as:

import helpers.hpandas as hpandas
"""

import logging
import os
from typing import Any, List, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import helpers.dbg as dbg
import helpers.io_ as hio
import helpers.introspection as hintro
import helpers.io_ as hio
import helpers.timer as htimer

_LOG = logging.getLogger(__name__)


def to_parquet(
    df: pd.DataFrame,
    file_name: str,
    *,
    log_level: int = logging.DEBUG,
) -> None:
    """
    Save a dataframe as Parquet.
    """
    dbg.dassert_isinstance(df, pd.DataFrame)
    dbg.dassert_isinstance(file_name, str)
    dbg.dassert_file_extension(file_name, "pq")
    #
    hio.create_enclosing_dir(file_name, incremental=True)
    _LOG.debug("df.shape=%s", str(df.shape))
    mem = df.memory_usage().sum()
    _LOG.debug("df.memory_usage=%s", hintro.format_size(mem))
    # Save data.
    with htimer.TimedScope(logging.DEBUG, "To parquet '%s'" % file_name) as ts:
        table = pa.Table.from_pandas(df)
        pq.write_table(table, file_name)
    # Report stats.
    file_size = hintro.format_size(os.path.getsize(file_name))
    _LOG.log(
        log_level,
        "Saved '%s' (size=%s, time=%.1fs)",
        file_name,
        file_size,
        ts.elapsed_time,
    )


def from_parquet(
    file_name: str,
    columns: Optional[List[str]] = None,
    filters: Optional[List[Any]] = None,
    *,
    log_level: int = logging.DEBUG,
) -> pd.DataFrame:
    """
    Load a dataframe from a Parquet file.
    """
    dbg.dassert_isinstance(file_name, str)
    dbg.dassert_file_extension(file_name, "pq")
    # Load data.
    with htimer.TimedScope(logging.DEBUG, "From parquet '%s'" % file_name) as ts:
        filesystem = None
        dataset = pq.ParquetDataset(
            file_name,
            filesystem=filesystem,
            filters=filters,
            use_legacy_dataset=False,
        )
        # To read also the index we need to use `read_pandas()`, instead of `read_table()`.
        # See https://arrow.apache.org/docs/python/parquet.html#reading-and-writing-single-files.
        table = dataset.read_pandas(columns=columns)
        df = table.to_pandas()
    # Report stats.
    file_size = hintro.format_size(os.path.getsize(file_name))
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
    _LOG.debug("df.memory_usage=%s", hintro.format_size(mem))
    return df
