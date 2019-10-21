import logging
import os

import boto3
import pandas as pd

import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)


# DUMMY_TO_REVIEW


def read_csv_range(csv_path, from_, to, **kwargs):
    """
    Read a specified row range of a csv file and convert to a DataFrame.

    Assumed to have header, considered to be row 0.
    Reads [from_, to), e.g., to - from_ lines.
    I.e., follows list slicing semantics.

    :param csv_path: location of csv file
    :param from_: first line to read (header is row 0 and is always read)
    :param to: last line to read, not inclusive
    :return: DataFrame with columns from csv line 0 (header)
    """
    dbg.dassert_lt(0, from_, msg="Row 0 assumed to be header row")
    dbg.dassert_lt(from_, to, msg="Empty range requested!")
    skiprows = range(1, from_)
    nrows = to - from_
    df = pd.read_csv(csv_path, skiprows=skiprows, nrows=nrows, **kwargs)
    if df.shape[0] < to:
        _LOG.warning("Number of df rows = %i vs requested = %i", df.shape[0], to)
    return df


def build_chunk(csv_path, col_name, start, nrows_at_a_time=1000, **kwargs):
    """
    Builds a DataFrame from a csv subset as follows:
      - Names the columns using the header line (row 0)
      - Reads the value in (row, col) coordinates (`start`, `col_name`) (if it
          exists) as `value`
      - Adds row `start` and all subsequent contiguous rows with `value` in
          column `col_name`

    For memory efficiency, the csv is processed in chunks of size
    `nrows_at_a_time`.

    :param csv_path: location of csv file
    :param col_name: name of column whose values define chunks
    :param start: first row to process
    :param nrows_at_a_time: size of chunks to process
    :return: DataFrame with columns from csv line 0
    """
    dbg.dassert_lt(0, start)
    stop = False
    dfs = []
    init_df = read_csv_range(csv_path, start, start + 1, **kwargs)
    if init_df.shape[0] < 1:
        return init_df
    val = init_df[col_name].iloc[0]
    _LOG.debug("Building chunk for %s", val)
    counter = 0
    while not stop:
        from_ = start + counter * nrows_at_a_time
        df = read_csv_range(csv_path, from_, from_ + nrows_at_a_time)
        # Break if there are no matches.
        if df.shape[0] == 0:
            break
        if not (df[col_name] == val).any():
            break
        # Stop if we have run out of rows to read.
        if df.shape[0] < nrows_at_a_time:
            stop = True
        idx_max = (df[col_name] == val)[::-1].idxmax()
        # Stop if we have reached a new value.
        if idx_max < (df.shape[0] - 1):
            stop = True
        dfs.append(df.iloc[0 : idx_max + 1])
        counter += 1
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, axis=0).reset_index(drop=True)


def find_first_matching_row(
    csv_path, col_name, val, start=1, nrows_at_a_time=1000000, **kwargs
):
    """
    Find first row in csv where value in column `col_name` equals `val`.

    :param csv_path: location of csv file
    :param col_name: name of column whose values define chunks
    :param val: value to match on
    :param start: first row (inclusive) to start search on
    :param nrows_at_a_time: size of chunks to process
    :return: line in csv of first matching row at or past start
    """
    curr = start
    while True:
        _LOG.debug("Start of current chunk = line %i", curr)
        df = read_csv_range(csv_path, curr, curr + nrows_at_a_time, **kwargs)
        if df.shape[0] < 1:
            _LOG.info("Value %s not found", val)
            break
        matches = df[col_name] == val
        if matches.any():
            idx_max = matches.idxmax()
            return curr + idx_max
        else:
            curr += nrows_at_a_time


def append(df, path, index=False, **kwargs):
    with open(path, "a") as f:
        df.to_csv(f, header=False, index=index, **kwargs)


def csv_mapreduce(
    csv_path, out_dir, key_func, chunk_preprocessor=None, chunksize=1000000
):
    """
    Map-reduce-type processing of csv. Here we
      - Read the csv in chunks, loading the chunk into a DataFrame
      - Key each row of the DataFrame using a groupby
      - "Reduce" keyed groups by writing and appending to a csv

    :param csv_path: input csv path
    :param out_dir: output dir for csv's with filenames corresponding to keys
    :param key_func: function to apply to each chunk DataFrame to key rows.
        Should return an iterable with elements like (key, df).
    :param chunk_preprocessor: optional. Function to apply to each chunk
        DataFrame before applying key_func.
    :param chunksize: chunksize of input to process
    :return: None
    """
    chunks = pd.read_csv(csv_path, chunksize=chunksize)
    if chunk_preprocessor is not None:
        chunks = map(chunk_preprocessor, chunks)
    keyed_group_blocks = map(key_func, chunks)
    for block in keyed_group_blocks:
        for idx, df in block:
            append(df, os.path.join(out_dir, idx + ".csv"))


def convert_csv_to_pq(csv_path, pq_path, normalizer=None, header=None):
    """
    Converts csv file to parquet file.

    Output of csv_mapreduce is typically headerless (to support append mode), and so
    `normalizer` may be used to add appropriate headers. Note that parquet
    requires string column names, whereas pandas by default uses integer
    column names.

    :param csv_path: full path of csv
    :param pq_path: full path of parquet
    :param header: header specification of csv
    :param normalizer: function to apply to df before writing to pq
    :return: None
    """
    # TODO(Paul): Ensure that one of header, normalizer is not None.
    df = pd.read_csv(csv_path, header=header)
    if normalizer is not None:
        df = normalizer(df)
    df.to_parquet(pq_path)


def _maybe_remove_extension(filename, extension):
    """
    Attempt to remove `extension` from `filename`.

    :param filename: str filename
    :param extension: e.g., ".csv"
    :return: returns a copy of filename but without `extension`, if applicable,
        else returns `None`.
    """
    dbg.dassert_isinstance(filename, str)
    # Assert filename is not empty.
    dbg.dassert(filename)
    #
    dbg.dassert_isinstance(extension, str)
    dbg.dassert(
        extension.startswith("."),
        "filename extension=`%s` expected to start with `.`",
        extension,
    )
    #
    if filename.endswith(extension):
        return filename[: -len(extension)]
    else:
        return None


def _list_s3_files(s3_path):
    AMAZON_MAX_INT = 2147483647
    split_path = s3_path.split("/")
    s3_bucket = split_path[2]
    dir_path = "/".join(split_path[3:])
    s3 = boto3.client("s3")
    s3_objects = s3.list_objects_v2(
        Bucket=s3_bucket, StartAfter=dir_path, MaxKeys=AMAZON_MAX_INT
    )
    contents = s3_objects["Contents"]
    file_names = [cont["Key"] for cont in contents]
    file_names = [
        os.path.basename(file_name)
        for file_name in file_names
        if os.path.dirname(file_name) == dir_path
    ]
    return file_names


def convert_csv_dir_to_pq_dir(csv_dir, pq_dir, normalizer=None, header=None):
    """
    Applies `convert_csv_to_pq` to all files in csv_dir

    :param csv_dir: directory of csv's
    :param pq_dir: target directory
    :param header: header specification of csv
    :param normalizer: function to apply to df before writing to pq
    :return: None
    """
    if not csv_dir.startswith("s3://"):
        dbg.dassert_exists(csv_dir)
        # TODO(Paul): check .endswith(".csv") or do glob(csv_dir + "/*.csv")
        filenames = os.listdir(csv_dir)
    else:
        filenames = _list_s3_files(csv_dir)
        dbg.dassert(filenames, "No files in the %s directory.", csv_dir)
    for filename in filenames:
        # Remove .csv/.csv.gz and add .pq.
        csv_stem = _maybe_remove_extension(filename, ".csv")
        if csv_stem is None:
            csv_stem = _maybe_remove_extension(filename, ".csv.gz")
        if csv_stem is None:
            # TODO(Paul): Consider making this a warning.
            _LOG.info("Skipping filename=%s", csv_stem)
            continue
        pq_filename = csv_stem + ".pq"
        convert_csv_to_pq(
            os.path.join(csv_dir, filename),
            os.path.join(pq_dir, pq_filename),
            normalizer=normalizer,
            header=header,
        )
