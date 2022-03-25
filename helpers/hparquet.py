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
        filesystem = get_pyarrow_s3fs(aws_profile)
        # Pyarrow S3FileSystem does not have `exists` method.
        s3_filesystem = hs3.get_s3fs(aws_profile)
        hs3.dassert_s3_path_exists(file_name, s3_filesystem)
        file_name = file_name.lstrip("s3://")
    else:
        filesystem = None
        hdbg.dassert_exists(file_name)
    # Load data.
    with htimer.TimedScope(
        logging.DEBUG, f"# Reading Parquet file '{file_name}'"
    ) as ts:
        dataset = pq.ParquetDataset(
            # Replace URI with path.
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


def to_parquet(
    df: pd.DataFrame,
    file_name: str,
    *,
    log_level: int = logging.DEBUG,
    report_stats: bool = False,
    aws_profile: hs3.AwsProfile = None,
) -> None:
    """
    Save a dataframe as Parquet.
    """
    hdbg.dassert_isinstance(df, pd.DataFrame)
    hdbg.dassert_isinstance(file_name, str)
    if aws_profile is not None:
        hdbg.dassert(hs3.is_s3_path(file_name))
        filesystem = hs3.get_s3fs(aws_profile)
        hs3.dassert_s3_path_exists(file_name, filesystem)
        file_name = file_name.lstrip("s3://")
    else:
        filesystem = None
        hdbg.dassert_not_exists(file_name)
    hdbg.dassert_file_extension(file_name, "parquet")
    # There is no concept of directory on S3.
    # Only applicable to local filesystem.
    if aws_profile is None:
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
        pq.write_table(table, file_name, filesystem=filesystem)
    # Report stats about the Parquet file size.
    # TODO(Nikola): CMTask1437 Extend hsystem.du to support S3.
    if report_stats and aws_profile is None:
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
    Yield Parquet data in tiles up to one year in length.

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
    Yield Parquet data in tiles up to one year in length.

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
    Use the year/months to build a Parquet filter.

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
    Report stats in a dataframe on Parquet file partitions.

    The directories should be of the form `lhs=rhs` where "rhs" is a string
    representation of an `int`.

    :param path: path to top-level Parquet directory
    :return: dataframe with two file size columns and a multiindex reflecting
        the Parquet path structure.
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
    or_and_filter = []
    if partition_mode == "by_year_month":
        # Handle the first and last year of the interval.
        if start_timestamp:
            # `[('year', '==', 2020), ('month', '>=', 6)]`
            and_filter = [
                ("year", "==", start_timestamp.year),
                ("month", ">=", start_timestamp.month),
            ]
            or_and_filter.append(and_filter)
        if end_timestamp:
            # `[('year', '==', 2021), ('month', '<=', 3)]`
            and_filter = [
                ("year", "==", end_timestamp.year),
                ("month", "<=", end_timestamp.month),
            ]
            or_and_filter.append(and_filter)
        if start_timestamp and end_timestamp:
            number_of_years = len(
                range(start_timestamp.year, end_timestamp.year + 1)
            )
            if number_of_years == 1:
                # For a one-year range, we overwrite the result with a single AND statement,
                # e.g., `[Jan 2020, Mar 2020]` corresponds to
                # `[[('year', '==', 2020), ('month', '>=', 1), ('month', '<=', 3)]]`.
                # Note that this interval is different from and OR-AND form as
                # `[[('year', '==', 2020), ('month', '>=', 1)], [('year', '==', 2020), ('month', '<=', 3)]]`
                # since the first AND clause include months <= 3 and the second one include months >= 1,
                # and the OR corresponds to the entire year, instead of the interval `[Jan 2020, Mar 2020]`.
                and_filter = [
                    ("year", "==", start_timestamp.year),
                    ("month", ">=", start_timestamp.month),
                    ("month", "<=", end_timestamp.month),
                ]
                or_and_filter = [and_filter]
            elif number_of_years > 2:
                # For ranges over two years, one OR statement is necessary to bridge the
                # gap between first and last AND statement.
                # `[('year', '>', 2020), ('year', '<', 2023)]`
                # Inserted in middle as bridge between AND statements.
                and_filter = [
                    ("year", ">", start_timestamp.year),
                    ("year", "<", end_timestamp.year),
                ]
                or_and_filter.insert(1, and_filter)
            else:
                # For intervals of exactly two years the two AND conditions are
                # enough to select the desired period of time.
                pass
        elif len(or_and_filter) == 1:
            # Handle the case when exactly one of the interval bounds is passed, e.g., [June 2020, None].
            # In this case the first year was covered by the code above (i.e,. `year >= 2020 and month == 6`)
            # and we need to specify the rest of the years (i.e., `year > 2020`).
            operator = ">" if start_timestamp else "<"
            timestamp = start_timestamp if start_timestamp else end_timestamp
            extra_filter = [("year", operator, timestamp.year)]
            or_and_filter.append(extra_filter)
        else:
            # If there is no interval provided, leave empty `or_and_filter` as is.
            pass
    elif partition_mode == "by_year_week":
        # TODO(gp): Consider using the same approach above for months also here.
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
        if or_and_filter:
            # Append additional filter for every present timestamp filter.
            or_and_filter = [
                [additional_filter] + and_filter for and_filter in or_and_filter
            ]
        else:
            # If no timestamp filters are provided, use additional filter.
            or_and_filter = [additional_filter]
    _LOG.debug("or_and_filter=%s", str(or_and_filter))
    if len(or_and_filter) == 0:
        # Empty list is not acceptable value for pyarrow dataset.
        # Only logical expression or `None`.
        or_and_filter = None
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
    partition_filename: Union[Callable, None] = lambda x: "data.parquet",
    aws_profile: hs3.AwsProfile = None,
) -> None:
    """
    Save the given dataframe as Parquet file partitioned along the given
    columns.

    :param df: dataframe
    :param partition_columns: partitioning columns
    :param dst_dir: location of partitioned dataset
    :param partition_filename: a callable to override standard partition names. None for `uuid`.
    :param aws_profile: the name of an AWS profile or a s3fs filesystem

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
    # Use either S3 or local filesystem.
    filesystem = None
    if aws_profile is not None:
        filesystem = hs3.get_s3fs(aws_profile)
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


# TODO(Nikola): Currently indirectly tested in
#  `im_v2/ccxt/data/extract/test/test_download_historical_data.py`.
def list_and_merge_pq_files(
    root_dir: str,
    *,
    file_name: str = "data.parquet",
    aws_profile: hs3.AwsProfile = None,
) -> None:
    """
    Merge all files of the Parquet dataset.

    Can be generalized to any used partition.

    The standard partition (also known as "by-tile") assumed is:

    ```
    root_dir/
        currency_pair=ADA_USDT/
            year=2021/
                month=12/
                    data.parquet
            year=2022/
                month=01/
                    data.parquet
        ...
        currency_pair=EOS_USDT/
            year=2021/
                month=12/
                    data.parquet
            year=2022/
                month=01/
                    data.parquet
    ```

    :param root_dir: root directory of Parquet dataset
    :param file_name: name of the single resulting file
    :param aws_profile: the name of an AWS profile or a s3fs filesystem
    """
    if aws_profile is not None:
        filesystem = hs3.get_s3fs(aws_profile)
    else:
        raise NotImplementedError("Local filesystem is not implemented!")
    # Get full paths to each Parquet file inside root dir.
    parquet_files = filesystem.glob(f"{root_dir}/**.parquet")
    _LOG.debug("Parquet files: '%s'", parquet_files)
    # Get paths only to the lowest level of dataset folders.
    dataset_folders = set(f.rsplit("/", 1)[0] for f in parquet_files)
    for folder in dataset_folders:
        # Get files per folder and merge if there are multiple ones.
        folder_files = filesystem.ls(folder)
        hdbg.dassert_ne(
            len(folder_files), 0, msg=f"Empty folder `{folder}` detected!"
        )
        if len(folder_files) == 1 and folder_files[0].endswith("/data.parquet"):
            # If there is already single `data.parquet` file, no action is required.
            continue
        # Read all files in target folder.
        data = pq.ParquetDataset(folder_files, filesystem=filesystem).read()
        # Remove all old files and write new, merged one.
        filesystem.rm(folder, recursive=True)
        pq.write_table(data, folder + "/" + file_name, filesystem=filesystem)


def maybe_cast_to_int(string: str) -> Union[str, int]:
    """
    Return `string` as an `int` if convertible, otherwise a no-op.

    This is useful for parsing mixed-type dataframe columns that may
    contain strings and ints. For example, a dataframe with columns
    `feature1, feature2, 1, 2, 3` will be written and read back with
    columns `1`, `2`, `3` as the strings "1", "2", "3" rather than the
    ints. This function can be used to rectify that in a post-processing
    column rename.
    """
    hdbg.dassert_isinstance(string, str)
    try:
        val = int(string)
    except ValueError:
        val = string
    return val
