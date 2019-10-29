import argparse
import logging
import os

import pandas as pd
from tqdm import tqdm

import helpers.csv as csv
import helpers.dbg as dbg
import helpers.s3 as hs3
import vendors.kibot.utils as kut

_LOG = logging.getLogger(__name__)


def compare():
    s3_path = hs3.get_path()
    kibot_dir_path = os.path.join(s3_path, "kibot")
    kibot_subdirs = hs3.listdir(kibot_dir_path, mode="non-recursive")
    _LOG.debug("Will compare files in the following dirs: %s", kibot_subdirs)
    #
    for subdir in tqdm(
        [
            "All_Futures_Continuous_Contracts_daily",
            "All_Futures_Contracts_daily",
            "All_Futures_Contracts_1min",
            "All_Futures_Continuous_Contracts_1min",
        ]
    ):
        subdir_path_csv = os.path.join(kibot_dir_path, subdir)
        subdir_path_pq = os.path.join(kibot_dir_path, "pq", subdir)
        for file_name in hs3.listdir(subdir_path_csv):
            # Read dataframe from csv.
            csv_path = os.path.join(subdir_path_csv, file_name)
            csv_df = kut.read_data(csv_path)
            csv_df.rename(columns={"date": "datetime"}, inplace=True)
            # Read dataframe from parquet.
            pq_file_name = (
                csv._maybe_remove_extension(file_name, ".csv.gz") + "pq"
            )
            pq_path = os.path.join(subdir_path_pq, pq_file_name)
            pq_df = pd.read_parquet(pq_path)
            # Compare the dataframes.
            dbg.dassert(
                csv_df.equals(pq_df),
                "The %s and %s dataframes are different",
                csv_path,
                pq_path,
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "-v",
        dest="log_level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level",
    )
    args = parser.parse_args()
    dbg.init_logger(args.log_level)
    #
    compare()
    _LOG.info("Compared the data.")
