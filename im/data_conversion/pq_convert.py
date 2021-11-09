#!/usr/bin/env python

"""
Read daily data from S3 in Parquet format and transform it into a different
Parquet representation.
"""

import argparse
import datetime
import glob
import logging
import os
import random

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from tqdm.autonotebook import tqdm

import helpers.dbg as dbg
import helpers.io_ as hio
import helpers.parser as prsr
import helpers.printing as hprint
import helpers.timer as htimer

# import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)

# #############################################################################


def _get_df(date) -> pd.DataFrame:
    """
    Create pandas random data, like:

    ```
                idx instr  val1  val2
    2000-01-01    0     A    99    30
    2000-01-02    0     A    54    46
    2000-01-03    0     A    85    86
    ```
    """
    instruments = "A B C D E".split()
    date = pd.Timestamp(date, tz="America/New_York")
    start_date = date.replace(hour=9, minute=30)
    end_date = date.replace(hour=16, minute=0)
    df_idx = pd.date_range(start_date, end_date, freq="5T")
    _LOG.debug("df_idx=[%s, %s]", min(df_idx), max(df_idx))
    _LOG.debug("len(df_idx)=%s", len(df_idx))
    random.seed(1000)
    # For each instruments generate random data.
    df = []
    for idx, inst in enumerate(instruments):
        df_tmp = pd.DataFrame(
            {
                "idx": idx,
                "instr": inst,
                "val1": [random.randint(0, 100) for k in range(len(df_idx))],
                "val2": [random.randint(0, 100) for k in range(len(df_idx))],
            },
            index=df_idx,
        )
        _LOG.debug(hprint.df_to_short_str("df_tmp", df_tmp))
        df.append(df_tmp)
    # Create a single df for all the instruments.
    df = pd.concat(df)
    _LOG.debug(hprint.df_to_short_str("df", df))
    return df


_PATH = "s3://eglp-core-data/ds/ext/bars/taq/v1.0-prod/60/"

# s3 = s3fs.S3FileSystem(profile="saml-spm-sasm")

# def get_available_dates():
#    """
#    Return list of all available dates.
#    """
#    #dates = pd.date_range(pd.Timestamp("2000-01-01"), pd.Timestamp("2000-01-15"), freq="1D")
#    # Essentially equivalent to `> aws s3 ls _PATH`
#    #dates = hs3.listdir(path, mode="non-recursive", aws_profile="saml-spm-sasm")
#    files = s3.ls(_PATH)
#    dates = map(os.path.basename, files)
#    dates = sorted(dates)
#    return dates


def read_data(date: str):
    path = os.path.join(_PATH, date, "data.parquet")
    dataset = pq.ParquetDataset(path, filesystem=s3, use_legacy_dataset=False)
    with htimer.TimedScope(logging.DEBUG, "Read source data"):
        table = dataset.read()
        df = table.to_pandas()
    with htimer.TimedScope(logging.DEBUG, "Add columns"):
        # Preserve int64 datetime columns, since they make csv read/writes easier.
        # Add new TZ-localized datetime columns for research and readability.
        for col_name in ["start_time", "end_time"]:
            df[col_name + "_ts"] = df[col_name].apply(
                datetime.datetime.fromtimestamp
            )
            df[col_name + "_ts"] = (
                df[col_name + "_ts"]
                .dt.tz_localize("UTC")
                .dt.tz_convert("America/New_York")
            )
        df.set_index("end_time_ts", inplace=True, drop=True)
    return df


def _save_data_as_pq(df, dst_dir):
    with htimer.TimedScope(logging.DEBUG, "Create partition idxs"):
        # Append year and month.
        df["year"] = df.index.year
        df["month"] = df.index.month
        df["day"] = df.index.day
    # Save.
    with htimer.TimedScope(logging.DEBUG, "Save data"):
        table = pa.Table.from_pandas(df)
        partition_cols = ["egid", "year", "month", "day"]
        pq.write_to_dataset(table, dst_dir, partition_cols=partition_cols)


def read_pq_data(dir_name: str) -> pd.DataFrame:
    with htimer.TimedScope(logging.DEBUG, "Read PQ data"):
        _LOG.debug("Reading pq file from '%s'", dir_name)
        dataset = ds.dataset(dir_name, format="parquet", partitioning="hive")
        _LOG.debug("Found %s files", len(dataset.files))
        df = dataset.to_table().to_pandas()
        _LOG.debug("Df shape=%s", str(df.shape))
    return df


def df_stats(df: pd.DataFrame) -> str:
    txt = []
    txt.append("df.shape=%s" % str(df.shape))
    txt.append("df.dtypes=%s" % str(df.dtypes))
    for col in df.columns:
        uniques = df[col].unique()
        num_unique = len(uniques)
        stats = "%s: num_unique=%s vals=%s" % (col, num_unique, str(uniques[:3]))
        txt.append(stats)
    val = "\n".join(txt)
    return val


def _date_exists(date: datetime.datetime, dst_dir: str) -> bool:
    """
    Check if the data corresponding to `date` under `dst_dir` already exists.
    """
    # /app/data/idx=0/year=2000/month=1/02e3265d515e4fb88ebe1a72a405fc05.parquet
    subdirs = glob.glob(f"{dst_dir}/idx=*")
    suffix = os.path.join("year=%s" % date.year, "month=%s" % date.month)
    _LOG.debug("suffix=%s", suffix)
    found = False
    for subdir in sorted(subdirs):
        file_name = os.path.join(subdir, suffix)
        exists = os.path.exists(file_name)
        _LOG.debug("file_name=%s -> exists=%s", file_name, exists)
        if exists:
            found = True
            break
    return found


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--start_date", action="store", help="Start date, e.g., 2010-01-01"
    )
    parser.add_argument(
        "--end_date", action="store", help="End date, e.g., 2010-01-01"
    )
    parser.add_argument("--incremental", action="store_true", help="")
    parser.add_argument("--dst_dir", action="store", help="Destination dir")
    prsr.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    # Insert your code here.
    # - Use _LOG.info(), _LOG.debug() instead of printing.
    # - Use dbg.dassert_*() for assertion.
    # - Use si.system() and si.system_to_string() to issue commands.
    dst_dir = args.dst_dir
    df = read_pq_data(dst_dir)
    print(df_stats(df))
    assert 0

    #
    hio.create_dir(dst_dir, incremental=args.incremental)
    # Get all the dates with s3.list
    dates = get_available_dates()
    # dbg.dassert_strictly_increasing_index(dates)
    # _LOG.info("Available dates=%s [%s, %s]", len(dates), min(dates), max(dates))
    # E.g., 20031002
    # Filter dates.
    def _convert_to_date(date: str) -> str:
        return pd.Timestamp(date).date().strftime("%Y%m%d")

    start_date, end_date = args.start_date, args.end_date
    if start_date is not None:
        start_date = _convert_to_date(start_date)
    else:
        start_date = min(dates)
    if end_date is not None:
        end_date = _convert_to_date(end_date)
    else:
        end_date = max(dates)
    dbg.dassert_lte(start_date, end_date)
    dates = [d for d in dates if start_date <= d <= end_date]
    dbg.dassert_lte(1, len(dates), "No dates were selected")
    _LOG.info("Filtered dates=%s [%s, %s]", len(dates), min(dates), max(dates))

    # Scan the dates.
    for date in tqdm(dates, desc="Running pq_convert"):
        if args.incremental and _date_exists(date, dst_dir):
            _LOG.info(
                "Skipping processing of date '%s since incremental mode'", date
            )
            continue
        # Read data.
        # df = _get_df(date)
        df = read_data(date)
        _LOG.debug("date=%s\ndf=\n%s", date, str(df.head(3)))
        _save_data_as_pq(df, dst_dir)

    read_pq_data(dst_dir)


if __name__ == "__main__":
    _main(_parse())
