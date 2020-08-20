#!/usr/bin/env python

r"""
Compare data among the two representations of Kibot data:
    - original: csv.gz
    - parquet: pq

Usage example:
> python vendors/kibot/compare_pq_to_csv.py -v DEBUG
"""

import argparse
import logging
import os

import pandas as pd
from tqdm import tqdm

import helpers.csv as csv
import helpers.dbg as dbg
import helpers.parser as prsr
import helpers.s3 as hs3
import vendors.kibot.utils as kut

_LOG = logging.getLogger(__name__)


def _compare():
    # Get the dirs to compare.
    s3_path = hs3.get_path()
    kibot_dir_path = os.path.join(s3_path, "kibot")
    kibot_subdirs = hs3.listdir(kibot_dir_path, mode="non-recursive")
    _LOG.debug("Compare files in the following dirs: %s", kibot_subdirs)
    #
    subdirs = [
        "All_Futures_Continuous_Contracts_daily",
        "All_Futures_Contracts_daily",
        "All_Futures_Contracts_1min",
        "All_Futures_Continuous_Contracts_1min",
    ]
    for subdir in tqdm(subdirs):
        subdir_path_csv = os.path.join(kibot_dir_path, subdir)
        subdir_path_pq = os.path.join(kibot_dir_path, "pq", subdir)
        files = hs3.listdir(subdir_path_csv)
        _LOG.debug("Found %d files to compare", len(files))
        for file_name in tqdm(files):
            # Read dataframe from csv.
            csv_path = os.path.join(subdir_path_csv, file_name)
            _LOG.debug("csv_path=%s", csv_path)
            # `kut._read_data` was used to read the data before it was
            # transformed to pq. The new official API does not support
            # reading the data without passing the frequency and
            # contract type.
            csv_df = kut._read_data(csv_path, nrows=None)
            csv_df.rename(columns={"date": "datetime"}, inplace=True)
            # Read dataframe from parquet.
            pq_file_name = (
                csv._maybe_remove_extension(file_name, ".csv.gz") + ".pq"
            )
            pq_path = os.path.join(subdir_path_pq, pq_file_name)
            _LOG.debug("pq_path=%s", pq_path)
            pq_df = pd.read_parquet(pq_path)
            # Compare the dataframes.
            if not csv_df.equals(pq_df):
                csv_df.to_csv("csv_df.csv")
                pq_df.to_csv("pq_df.csv")
                raise ValueError("The dataframes are different: saved in files")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    prsr.add_verbosity_arg(parser)
    args = parser.parse_args()
    dbg.init_logger(args.log_level)
    dbg.shutup_chatty_modules()
    #
    _compare()
    _LOG.info("Compared the data.")
