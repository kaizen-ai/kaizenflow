"""
Import as:

import helpers.hparquet as hparque
"""

import collections
import datetime
import logging
import os
from typing import Any, Callable, Iterator, List, Optional, Tuple, Union

import pandas as pd
import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
from tqdm.autonotebook import tqdm

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hintrospection as hintros
import helpers.hprint as hprint
import helpers.hs3 as hs3
import helpers.hsystem as hsystem
import helpers.htimer as htimer

_LOG = logging.getLogger(__name__)


def get_pyarrow_s3fs(*args: Any, **kwargs: Any) -> pafs.S3FileSystem:
    """
    Return an Pyarrow S3Fs object from a given AWS profile.

    Same as `hs3.get_s3fs`, used specifically for accessing Parquet
    datasets.
    """
    aws_credentials = hs3.get_aws_credentials(*args, **kwargs)
    s3fs_ = pafs.S3FileSystem(
        access_key=aws_credentials["aws_access_key_id"],
        secret_key=aws_credentials["aws_secret_access_key"],
        session_token=aws_credentials["aws_session_token"],
        region=aws_credentials["aws_region"],
    )
    return s3fs_


def from_parquet(
    file_name: str,
    *,
    columns: Optional[List[str]] = None,
    filters: Optional[List[Any]] = None,
    log_level: int = logging.DEBUG,
    report_stats: bool = False,
    aws_profile: Optional[str] = None,
) -> pd.DataFrame:
    """
    Load a dataframe from a Parquet file.

    The difference with `pd.read_pq` is that here we use Parquet
    Dataset.
    """
    _LOG.debug(hprint.to_str("file_name columns filters"))
    hdbg.dassert_isinstance(file_name, str)
    if aws_profile is not None:
        hdbg.dassert(hs3.is_s3_path(file_name))
        fs = get_pyarrow_s3fs(aws_profile)
        file_name = file_name.lstrip("s3://")
        # TODO(Danya): pyarrow S3FileSystem does not have `exists` method
        #  for assertion.
    else:
        fs = None
        hdbg.dassert_exists(file_name)
    # Load data.
    with htimer.TimedScope(
        logging.DEBUG, f"# Reading Parquet file '{file_name}'"
    ) as ts:
        dataset = pq.ParquetDataset(
            # Replace URI with path.
            file_name,
            filesystem=fs,
            filters=filters,
            use_legacy_dataset=False,
        )
        # To read also the index we need to use `read_pandas()`, instead of
        # `read_table()`.
        # See https://arrow.apache.org/docs/python/parquet.html#reading-and-writing-single-files.
        table = dataset.read_pandas(columns=columns)
        df = table.to_pandas()
    # Report stats about the df.
    _LOG.debug("df.shape=%s", str(df.shape))
    mem = df.memory_usage().sum()
    _LOG.debug("df.memory_usage=%s", hintros.format_size(mem))
    # Report stats about the Parquet file size.
    if report_stats:
        file_size = hsystem.du(file_name, human_format=True)
        _LOG.log(
            log_level,
            "Loaded '%s' (size=%s, time=%.1fs)",
            file_name,
            file_size,
            ts.elapsed_time,
        )
    return df


# Copied from `hio.create_enclosing_dir()` to avoid circular dependencies.
def _create_enclosing_dir(file_name: str) -> str:
    dir_name = os.path.dirname(file_name)
    if dir_name != "":
        _LOG.debug(
            "Creating dir_name='%s' for file_name='%s'", dir_name, file_name
        )
        hdbg.dassert_is_not(dir_name, None)
        dir_name = os.path.normpath(dir_name)
        if os.path.normpath(dir_name) == ".":
            _LOG.debug("Can't create dir '%s'", dir_name)
        if os.path.exists(dir_name):
            # The dir exists and we want to keep it, so we are done.
            _LOG.debug("The dir '%s' exists: exiting", dir_name)
            return
        _LOG.debug("Creating directory '%s'", dir_name)
        try:
            os.makedirs(dir_name)
        except OSError as e:
            _LOG.error(str(e))
            # It can happen that we try to create the directory while somebody else
            # created it, so we neutralize the corresponding exception.
            if e.errno == 17:
                # OSError: [Errno 17] File exists.
                pass
            else:
                raise e
    hdbg.dassert_dir_exists(dir_name, "file_name='%s'", file_name)
    return dir_name


# TODO(gp): @Nikola allow to read / write from S3 passing aws_profile like done
#  in the rest of the code.
def to_parquet(
    df: pd.DataFrame,
    file_name: str,
    *,
    log_level: int = logging.DEBUG,
    report_stats: bool = False,
) -> None:
    """
    Save a dataframe as Parquet.
    """
    hdbg.dassert_isinstance(df, pd.DataFrame)
    hdbg.dassert_isinstance(file_name, str)
    hdbg.dassert_file_extension(file_name, ["pq", "parquet"])
    _create_enclosing_dir(file_name)
    # Report stats about the df.
    _LOG.debug("df.shape=%s", str(df.shape))
    mem = df.memory_usage().sum()
    _LOG.debug("df.memory_usage=%s", hintros.format_size(mem))
    # Save data.
    with htimer.TimedScope(
        logging.DEBUG, f"# Writing Parquet file '{file_name}'"
    ) as ts:
        table = pa.Table.from_pandas(df)
        pq.write_table(table, file_name)
    # Report stats about the Parquet file size.
    if report_stats:
        file_size = hsystem.du(file_name, human_format=True)
        _LOG.log(
            log_level,
            "Saved '%s' (size=%s, time=%.1fs)",
            file_name,
            file_size,
            ts.elapsed_time,
        )


# #############################################################################


def yield_parquet_tiles_by_year(
    file_name: str,
    start_date: datetime.date,
    end_date: datetime.date,
    cols: List[Union[int, str]],
) -> Iterator[pd.DataFrame]:
    """
    Yield parquet data in tiles up to one year in length.

    :param file_name: as in `from_parquet()`
    :param start_date: first date to load; day is ignored
    :param end_date: last date to load; day is ignored
    :param cols: if an `int` is supplied, it is cast to a string before reading
    :return: a generator of `from_parquet()` dataframes
    """
    filters = build_year_month_filter(start_date, end_date)
    hdbg.dassert_isinstance(filters, list)
    # The list should not be empty.
    hdbg.dassert(filters)
    if not isinstance(filters[0], list):
        filters = [filters]
    columns = [str(col) for col in cols]
    for filter_ in filters:
        tile = from_parquet(
            file_name,
            columns=columns,
            filters=filter_,
        )
        yield tile


def build_asset_id_filter(
    asset_ids: List[int],
    asset_id_col: str,
) -> List[List[Tuple[str, str, int]]]:
    filters = []
    for asset_id in asset_ids:
        filters.append([(asset_id_col, "==", asset_id)])
    return filters


# TODO(Paul): Add additional time-restriction filter.
def yield_parquet_tiles_by_assets(
    file_name: str,
    asset_ids: List[int],
    asset_id_col: str,
    asset_batch_size: int,
    cols: List[Union[int, str]],
) -> Iterator[pd.DataFrame]:
    """
    Yield parquet data in tiles up to one year in length.

    :param file_name: as in `from_parquet()`
    :param cols: if an `int` is supplied, it is cast to a string before reading
    :return: a generator of `from_parquet()` dataframes
    """
    batches = [
        asset_ids[i : i + asset_batch_size]
        for i in range(0, len(asset_ids), asset_batch_size)
    ]
    columns = [str(col) for col in cols]
    for batch in tqdm(batches):
        _LOG.debug("assets=%s", batch)
        filter_ = build_asset_id_filter(batch, asset_id_col)
        tile = from_parquet(
            file_name,
            columns=columns,
            filters=filter_,
        )
        yield tile


def build_year_month_filter(
    start_date: datetime.date,
    end_date: datetime.date,
) -> list:
    """
    Use the year/months to build a parquet filter.

    If `start_date.year == end_date.year`, then return a list of
    three tuples (to be "ANDed" together) based on the year and months.
    Else, return a list of list of tuples:
      - the inner lists consist of AND filters; the inner lists are ORed
        together if used as a single filter
      - each inner list filter represents a calendar year or part thereof

    One use case of this function is to generate a filter whose OR
    components can be processed one-by-one. For example, if memory constraints
    prevent loading an entire tile at once, then one could instead attempt to
    load one-year tiles one at a time.

    NOTE: `start_date.day` and `end_date.day` are ignored.

    TODO(Paul): Consider adding a switch to support smaller AND filter chunks
    (e.g., at monthly instead of yearly granularity).
    """
    hdbg.dassert_isinstance(start_date, datetime.date)
    hdbg.dassert_isinstance(end_date, datetime.date)
    hdbg.dassert_lte(start_date, end_date)
    start_year = start_date.year
    end_year = end_date.year
    filter_ = []
    #
    if start_year == end_year:
        filter_.append(("year", "==", start_year))
        filter_.append(("month", ">=", start_date.month))
        filter_.append(("month", "<=", end_date.month))
    else:
        start_year_filter = []
        start_year_filter.append(("year", "==", start_year))
        start_year_filter.append(("month", ">=", start_date.month))
        end_year_filter = []
        end_year_filter.append(("year", "==", end_year))
        end_year_filter.append(("month", "<=", end_date.month))
        filter_.append(start_year_filter)
        filter_.append(end_year_filter)
    for year in range(start_year + 1, end_year):
        year_filter = []
        year_filter.append(("year", "==", year))
        filter_.append(year_filter)
    return filter_


def collate_parquet_tile_metadata(
    path: str,
) -> pd.DataFrame:
    """
    Report stats in a dataframe on parquet file partitions.

    The directories should be of the form `lhs=rhs` where "rhs" is a string
    representation of an `int`.

    :param path: path to top-level parquet directory
    :return: dataframe with two file size columns and a multiindex reflecting
        the parquet path structure.
    """
    hdbg.dassert_dir_exists(path)
    # Remove the trailing slash to simplify downstream accounting.
    if path.endswith("/"):
        path = path[:-1]
    hdbg.dassert(not path.endswith("/"))
    # Walk the path.
    # os.walk() yields a 3-tuple of the form
    #  (dirpath: str, dirnames: List[str], filenames: List[str])
    start_depth = len(path.split("/"))
    headers_set = set()
    dict_ = collections.OrderedDict()
    for triple in os.walk(path):
        # If the walk has taken us to, e.g.,
        #     asset_id=100/year=2010/month=1/data.parquet
        # then we expect
        #     lhs = ("asset_id", "year", "month")
        #     rhs = (100, 2010, 1)
        lhs, rhs = _process_walk_triple(triple, start_depth)
        # If the walkabout has not yet taken us to a file, continue.
        if not lhs:
            continue
        # The tuple `lhs` is to become the index headers. We check later
        # for uniqueness.
        headers_set.add(lhs)
        # Get the file name and full path.
        file_name = triple[2][0]
        file_path = os.path.join(triple[0], file_name)
        # Record the size of the file. We keep this in bytes for easy
        # join aggregations.
        size_in_bytes = os.path.getsize(file_path)
        dict_[rhs] = size_in_bytes
    # Ensure that headers are unambiguous.
    hdbg.dassert_eq(len(headers_set), 1)
    # Convert to a multiindexed dataframe.
    df = pd.DataFrame(dict_.values(), index=dict_.keys())
    df.rename(columns={0: "file_size_in_bytes"}, inplace=True)
    headers = headers_set.pop()
    df.index.names = headers
    df.sort_index(inplace=True)
    # Add a more human-readable file size column. Keep the original numerical
    # one for downstream aggregations.
    file_size = df["file_size_in_bytes"].apply(hintros.format_size)
    df["file_size"] = file_size
    return df


# TODO(Paul): The `int` assumption is baked in. We can generalize to strings
#  if needed, but if we do, then we should continue to handle string ints as
#  ints as we do here (e.g., there are sorting advantages, among others).
def _process_walk_triple(
    triple: tuple, start_depth
) -> Tuple[Tuple[str], Tuple[int]]:
    """
    Process a triple returned by `os.walk()`

    :param triple: (dirpath: str, dirnames: List[str], filenames: List[str])
    :param start_depth: the "depth" of `path` used in the call
        `os.walk(path)`
    :return: tuple(lhs_vals), tuple(rhs_vals)
    """
    lhs_vals = []
    rhs_vals = []
    # If there are subdirectories, do not process.
    if triple[1]:
        return tuple(lhs_vals), tuple(rhs_vals)
    depth = len(triple[0].split("/"))
    rel_depth = depth - start_depth
    key = tuple(triple[0].split("/")[start_depth:])
    if len(key) == 0:
        return tuple(lhs_vals), tuple(rhs_vals)
    hdbg.dassert_eq(len(key), rel_depth)
    lhs_vals = []
    rhs_vals = []
    for string in key:
        lhs, rhs = string.split("=")
        lhs_vals.append(lhs)
        rhs_vals.append(int(rhs))
    hdbg.dassert_eq(len(lhs_vals), len(rhs_vals))
    return tuple(lhs_vals), tuple(rhs_vals)


# #############################################################################

# A Parquet filtering condition. e.g., `("year", "=", year)`
ParquetFilter = Tuple[str, str, Any]
# The AND of Parquet filtering conditions, e.g.,
#   `[("year", "=", year), ("month", "=", month)]`
ParquetAndFilter = List[ParquetFilter]
# A OR-AND Parquet filtering condition, e.g.,
#   ```
#   [[('year', '=', 2020), ('month', '=', 1)],
#    [('year', '=', 2020), ('month', '=', 2)],
#    [('year', '=', 2020), ('month', '=', 3)]]
#   ```
ParquetOrAndFilter = List[ParquetAndFilter]


# TODO(gp): @Nikola add light unit tests for `by_year_week` and for additional_filter.
def get_parquet_filters_from_timestamp_interval(
    partition_mode: str,
    start_timestamp: Optional[pd.Timestamp],
    end_timestamp: Optional[pd.Timestamp],
    *,
    additional_filter: Optional[ParquetFilter] = None,
) -> ParquetOrAndFilter:
    """
    Convert a constraint on a timestamp [start_timestamp, end_timestamp] into a
    Parquet filters expression, based on the passed partitioning / tiling
    criteria.

    :param partition_mode: control filtering of Parquet datasets. It needs to be
        in sync with the way the data was saved
    :param start_timestamp: start of the interval. `None` means no bound
    :param end_timestamp: end of the interval. `None` means no bound
    :param additional_filter: an AND condition to add to the final filter.
        E.g., if we want to constraint also on `asset_ids`, we can specify
        `("asset_id", "in", (...))`
    :return: list of OR-AND predicates
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
    # Use hardwired start and end date to represent start_timestamp /
    # end_timestamp = None. This is not very elegant, but it simplifies the code.
    # TODO(gp): This approach of enumerating seems slow when Parquet reads the data.
    #  Verify it is and then use a smarter approach like year <= ...
    if start_timestamp is None:
        start_timestamp = pd.Timestamp("2001-01-01 00:00:00+00:00")
    if end_timestamp is None:
        end_timestamp = pd.Timestamp("2030-01-01 00:00:00+00:00")
    or_and_filter = []
    if partition_mode == "by_year_month":
        # Partition by year and month.
        # Include last month in the interval.
        end_timestamp += pd.DateOffset(months=1)
        # Get all months in interval.
        dates = pd.date_range(
            start_timestamp.date(), end_timestamp.date(), freq="M"
        )
        years = list(set(dates.year))
        if len(years) == 1:
            # For one year range, a simple AND statement is enough.
            # `[[('year', '==', 2020), ('month', '>=', 6), ('month', '<=', 12)]]`
            and_filter = [
                ("year", "==", dates[0].year),
                ("month", ">=", dates[0].month),
                ("month", "<=", dates[-1].month),
            ]
            or_and_filter.append(and_filter)
        else:
            # For ranges over two years, OR statements are necessary to bridge the
            # gap between first and last AND statement, unless range is around two years.
            # First AND filter.
            first_and_filter = [
                ("year", "==", dates[0].year),
                ("month", ">=", dates[0].month),
                ("year", "==", dates[0].year),
                ("month", "<=", 12),
            ]
            or_and_filter.append(first_and_filter)
            # OR statements to bridge the gap, if any.
            # `[('year', '==', 2021)]`
            for year in years[1:-1]:
                bridge_and_filter = [("year", "==", year)]
                or_and_filter.append(bridge_and_filter)
            # Last AND filter.
            last_and_filter = [
                ("year", "==", dates[-1].year),
                ("month", ">=", 1),
                ("year", "==", dates[-1].year),
                ("month", "<=", dates[-1].month),
            ]
            or_and_filter.append(last_and_filter)
    elif partition_mode == "by_year_week":
        # Partition by year and week.
        # Include last week in the interval.
        end_timestamp += pd.DateOffset(weeks=1)
        # Get all weeks in the interval.
        dates = pd.date_range(
            start_timestamp.date(), end_timestamp.date(), freq="W"
        )
        for date in dates:
            year = date.year
            # https://docs.python.org/3/library/datetime.html#datetime.date.isocalendar
            weekofyear = date.isocalendar()[1]
            and_filter = [("year", "=", year), ("weekofyear", "=", weekofyear)]
            or_and_filter.append(and_filter)
    else:
        raise ValueError(f"Unknown partition mode `{partition_mode}`!")
    if additional_filter:
        hdbg.dassert_isinstance(additional_filter, tuple)
        or_and_filter = [
            [additional_filter] + and_filter for and_filter in or_and_filter
        ]
    _LOG.debug("or_and_filter=%s", str(or_and_filter))
    return or_and_filter


def add_date_partition_columns(
    df: pd.DataFrame, partition_mode: str
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Add partition columns like year, month, day from datetime index.

    :param df: dataframe indexed by timestamp
    :param partition_mode:
        - "by_date": extract the date from the index
            - E.g., an index like `2022-01-10 14:00:00+00:00` is transform to a
              column `20220110`
        - "by_year_month_day": split the index in year, month, day columns
        - "by_year_month": split by year and month
        - "by_year_week": split by year and week of the year
        - "by_year": split by year
    :return:
        - df with additional partitioning columns
        - list of partitioning columns
    """
    with htimer.TimedScope(logging.DEBUG, "# add_date_partition_cols"):
        if partition_mode == "by_date":
            df["date"] = df.index.strftime("%Y%m%d")
            partition_columns = ["date"]
        else:
            if partition_mode == "by_year_month_day":
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
            # Add date columns chosen by partition mode.
            for column_name in partition_columns:
                # Extract data corresponding to `column_name` (e.g.,
                # `df.index.year`).
                df[column_name] = getattr(df.index, column_name)
    return df, partition_columns


def to_partitioned_parquet(
    df: pd.DataFrame,
    partition_columns: List[str],
    dst_dir: str,
    *,
    filesystem=None,
    partition_filename: Union[Callable, None] = lambda x: "data.parquet",
) -> None:
    """
    Save the given dataframe as Parquet file partitioned along the given
    columns.

    :param df: dataframe
    :param partition_columns: partitioning columns
    :param dst_dir: location of partitioned dataset
    :param filesystem: filesystem to use (e.g. S3FS), if None, local FS is assumed
    :param partition_filename: a callable to override standard partition names. None for `uuid`.

    E.g., in case of partition using `date`, the file layout looks like:
    ```
    dst_dir/
        date=20211230/
            data.parquet
        date=20211231/
            data.parquet
        date=20220101/
            data.parquet
    ```

    In case of multiple columns like `asset`, `year`, `month`, the file layout
    looks like:
    ```
    dst_dir/
        asset=A/
            year=2021/
                month=12/
                    data.parquet
            year=2022/
                month=01/
                    data.parquet
        ...
        asset=B/
            year=2021/
                month=12/
                    data.parquet
            year=2022/
                month=01/
                    data.parquet
    ```
    """
    with htimer.TimedScope(logging.DEBUG, "# partition_dataset"):
        # Read.
        table = pa.Table.from_pandas(df)
        # Write using partition.
        # TODO(gp): add this logic to hparquet.to_parquet as a possible option.
        _LOG.debug(hprint.to_str("partition_columns dst_dir"))
        hdbg.dassert_is_subset(partition_columns, df.columns)
        pq.write_to_dataset(
            table,
            dst_dir,
            partition_cols=partition_columns,
            partition_filename_cb=partition_filename,
            filesystem=filesystem,
        )


def maybe_cast_to_int(string: str) -> Union[str, int]:
    """
    Return `string` as an `int` if convertible, otherwise a no-op.

    This is useful for parsing mixed-type dataframe columns that may contain
    strings and ints. For example, a dataframe with columns
       feature1 feature2 1 2 3
    will be written and read back with columns `1`, `2`, `3` as the strings
    "1", "2", "3" rather than the ints. This function can be used to rectify
    that in a post-processing column rename.
    """
    hdbg.dassert_isinstance(string, str)
    try:
        val = int(string)
    except ValueError:
        val = string
    return val
