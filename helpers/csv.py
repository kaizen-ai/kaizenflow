import logging
import os
from typing import Any, Callable, Dict, Optional, Union

import pandas as pd
import ast

import helpers.dbg as dbg
import helpers.io_ as io_
import helpers.s3 as hs3

_LOG = logging.getLogger(__name__)


def read_csv_range(
    csv_path: str, from_: int, to: int, **kwargs: Any
) -> pd.DataFrame:
    """Read a specified row range of a csv file and convert to a DataFrame.

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


def build_chunk(
    csv_path: str,
    col_name: str,
    start: int,
    nrows_at_a_time: int = 1000,
    **kwargs: Any,
) -> pd.DataFrame:
    """Build a DataFrame from a csv subset as follows:

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
    csv_path: str,
    col_name: str,
    val: str,
    start: int = 1,
    nrows_at_a_time: int = 1000000,
    **kwargs: Any,
) -> Optional[int]:
    """Find first row in csv where value in column `col_name` equals `val`.

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
        curr += nrows_at_a_time
    return None


def append(
    df: pd.DataFrame, path: str, index: bool = False, **kwargs: Any
) -> None:
    with open(path, "a") as f:
        df.to_csv(f, header=False, index=index, **kwargs)


def csv_mapreduce(
    csv_path: str,
    out_dir: str,
    key_func: Callable,
    chunk_preprocessor: Union[None, Callable],
    chunksize: int = 1000000,
) -> None:
    """Map-reduce-type processing of csv.

    The phases are:
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


def convert_csv_to_pq(
    csv_path: str,
    pq_path: str,
    normalizer: Union[Callable, None] = None,
    header: Union[None, str] = None,
    compression: Optional[str] = "gzip",
) -> None:
    """Convert csv file to parquet file.

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
    df.to_parquet(pq_path, compression=compression)


# TODO(gp): Promote to io_.
def _maybe_remove_extension(filename: str, extension: str) -> Optional[str]:
    """Attempt to remove `extension` from `filename`.

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
    ret: Optional[str] = None
    if filename.endswith(extension):
        ret = filename[: -len(extension)]
    return ret


def convert_csv_dir_to_pq_dir(
    csv_dir: str,
    pq_dir: str,
    normalizer: Union[None, Callable] = None,
    header: Union[None, str] = None,
) -> None:
    """Apply `convert_csv_to_pq` to all files in csv_dir.

    :param csv_dir: directory of csv's
    :param pq_dir: target directory
    :param header: header specification of csv
    :param normalizer: function to apply to df before writing to pq
    :return: None
    """
    if not hs3.is_s3_path(csv_dir):
        dbg.dassert_exists(csv_dir)
        # TODO(Paul): check .endswith(".csv") or do glob(csv_dir + "/*.csv")
        filenames = os.listdir(csv_dir)
    else:
        filenames = hs3.listdir(csv_dir)
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


def convert_csv_to_dict(path_to_csv: str, remove_nans: bool) -> Dict[Any, Any]:
    """Convert a csv file with a dataframe into a json-compatible dict.

    :param path_to_csv: path to the csv file
    :param remove_nans: whether to remove NaNs from the dictionary
    :return: a json-compatible dict with the dataframe data
    """
    dbg.dassert_exists(
        path_to_csv, "The file '%s' is not found.", path_to_csv,
    )
    # Load the dataframe from a csv file.
    df = pd.read_csv(path_to_csv)
    # Transform the dataframe into a dict.
    dict_df = df.to_dict(orient="list")
    if remove_nans:
        # Remove NaNs from the dict.
        for key in dict_df:
            dict_df[key] = [x for x in dict_df[key] if not pd.isnull(x)]
    return dict_df  # type: ignore


def save_csv_as_json(
    path_to_csv: str, remove_nans: bool, path_to_json: Optional[str] = None
) -> None:
    """Convert the df from a csv into a dict and save it into a json file.

    If the `path_to_json` is not provided, the json is saved in the folder where
    the csv file is located.

    :param path_to_csv: path to the csv file
    :param remove_nans: whether to remove NaNs from the dictionary
    :param path_to_json: path to save the json file
    :return:
    """
    # Convert the df from the csv into a json-compatible dict.
    dict_df = convert_csv_to_dict(path_to_csv, remove_nans)
    # Determine the json destination path.
    if path_to_json is None:
        path_to_json = io_.change_filename_extension(path_to_csv, ".csv", ".json")
    # Save the dict into a json file.
    io_.to_json(path_to_json, dict_df)


def to_typed_csv(df: pd.DataFrame, file_name: str) -> None:
    """Convert Dataframe into csv and then creates a file with the dtypes of columns.
    
    As the file with types, this function create file with the same name and suffix
    'foobar.csv.types'.

    :param df: dataframe, which you want to convert into csv.
    :param file_name: name of file with desired format, which is used for saving
    :return: 
    """
    dtypes_filename = file_name + '.types'
    io_.create_enclosing_dir(dtypes_filename, incremental=True)
    dtypes_dict = str(df.dtypes.apply(lambda x: x.name).to_dict())

    df.to_csv(file_name, index = False)
    with open(dtypes_filename, 'w') as dtypes_file:
        dtypes_file.write(dtypes_dict)
    

def from_typed_csv(file_name: str) -> pd.DataFrame:
    """Loads csv file into dataframe and applies the original types of columns,
    in order to open csv in a proper way.

    As a file, which contains types format, it is used 'file_name.types' file,
    if it's exist.

    :param file_name: name of file, which is need to be converted into dataframe
    :return pd.DataFrame: dataframe of pandas format.
    """
    dtypes_filename = file_name + '.types'
    dbg.dassert_dir_exists(dtypes_filename)

    dtypes_file = open(dtypes_filename)
    dtypes_dict = ast.literal_eval(list(dtypes_file)[0])

    df = pd.read_csv(file_name, dtype = dtypes_dict)
    return df
