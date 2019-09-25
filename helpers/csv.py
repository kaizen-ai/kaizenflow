import logging
import os

import pandas as pd

import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)


def read_csv_range(csv_path, from_, to, **kwargs):
    """
    Read a specified row range of a csv file and convert to a DataFrame.

    Assumed to have header, considered to be row 0.
    Reads [from_, to), e.g., to - from_ lines.
    I.e., follows list slicing semantics.

    :param csv_path: Location of csv file
    :param from_: First line to read (header is row 0 and is always read)
    :param to: Last line to read, not inclusive
    :return: DataFrame with columns from csv line 0 (header)
    """
    dbg.dassert_lt(0, from_, msg="Row 0 assumed to be header row")
    dbg.dassert_lt(from_, to, msg="Empty range requested!")
    skiprows = [i for i in range(1, from_)]
    nrows = to - from_
    df = pd.read_csv(csv_path, skiprows=skiprows, nrows=nrows, **kwargs)
    if df.shape[0] < to:
        _LOG.info("Number of df rows = %i vs requested = %i", df.shape[0], to)
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

    :param csv_path: Location of csv file
    :param col_name: Name of column whose values define chunks
    :param start: First row to process
    :param nrows_at_a_time: Size of chunks to process
    :return: DataFrame with columns from csv line 0
    """
    dbg.dassert_lt(0, start)
    # _LOG.info("row = %i", start)
    stop = False
    dfs = []
    init_df = read_csv_range(csv_path, start, start + 1, **kwargs)
    if init_df.shape[0] < 1:
        return init_df
    val = init_df[col_name].iloc[0]
    _LOG.info("Building chunk for %s", val)
    counter = 0
    while not stop:
        from_ = start + counter * nrows_at_a_time
        df = read_csv_range(csv_path, from_, from_ + nrows_at_a_time)
        # Break if there are no matches.
        if df.shape[0] == 0:
            break
        if not (df[col_name] == val).any():
            break
        # Stop if we have run out of rows to read
        if df.shape[0] < nrows_at_a_time:
            stop = True
        idx_max = (df[col_name] == val)[::-1].idxmax()
        # Stop if we have reached a new value
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

    :param csv_path: Location of csv file
    :param col_name: Name of column whose values define chunks
    :param val: Value to match on
    :param start: First row (inclusive) to start search on
    :param nrows_at_a_time: Size of chunks to process
    :return: Line in csv of first matching row at or past start
    """
    curr = start
    while True:
        _LOG.info("Start of current chunk = line %i", curr)
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


def csv_mr(
    csv_path, out_dir, key_func, chunk_preprocessor=None, chunksize=1000000
):
    """
    Map-reduce-type processing of csv. Here we
      - Read the csv in chunks, loading the chunk into a DataFrame
      - Key each row of the DataFrame using a groupby
      - "Reduce" keyed groups by writing and appending to a csv

    :param csv_path: Input csv path
    :param out_dir: Output dir for csv's with filenames corresponding to keys
    :param key_func: Function to apply to each chunk DataFrame to key rows.
        Should return an iterable with elements like (key, df).
    :param chunk_preprocessor: Optional. Function to apply to each chunk
        DataFrame before applying key_func.
    :param chunksize: Chunksize of input to process
    :return: None
    """
    chunks = pd.read_csv(csv_path, chunksize=chunksize)
    if chunk_preprocessor is not None:
        chunks = map(chunk_preprocessor, chunks)
    keyed_group_blocks = map(key_func, chunks)
    for block in keyed_group_blocks:
        for idx, df in block:
            append(df, os.path.join(out_dir, idx + ".csv"))


def csv_to_pq(csv_path, pq_path, normalizer=None, header=None):
    """
    Converts csv file to parquet file.

    Output of csv_mr is typically headerless (to support append mode), and so
    `normalizer` may be used to add appropriate headers. Note that parquet
    requires string column names, whereas pandas by default uses integer
    column names.

    :param csv_path: Full path of csv
    :param pq_path: Full path of parquet
    :param header: Header specification of csv
    :param normalizer: Function to apply to df before writing to pq
    :return: None
    """
    # TODO(Paul): Ensure that one of header, normalizer is not None.
    df = pd.read_csv(csv_path, header=header)
    if normalizer is not None:
        df = normalizer(df)
    df.to_parquet(pq_path)


def csv_dir_to_pq_dir(csv_dir, pq_dir, normalizer=None):
    """
    Applies `csv_to_pq` to all files in csv_dir

    :param csv_dir: Directory of csv's
    :param pq_dir: Target directory
    :param normalizer: Function to apply to df before writing to pq
    :return: None
    """
    filenames = os.listdir(csv_dir)
    for filename in filenames:
        # Remove .csv and add .pq
        pq_filename = filename[:-4] + ".pq"
        csv_to_pq(
            os.path.join(csv_dir, filename),
            os.path.join(pq_dir, pq_filename),
            normalizer,
        )

