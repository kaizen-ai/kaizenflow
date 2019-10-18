#!/usr/bin/env python

r"""
Download equity data from the http://firstratedata.com.

- Save the data as zipped CSVs for each equity to one of the
    - commodity
    - crypto
    - fx
    - index
    - stock_A-D
    - stock_E-I
    - stock_J-N
    - stock_O-R
    - stock_S-Z
  category directories.
- Combine zipped CSVs for each equity, add "timestamp" column and
  column names. Save as CSV to corresponding category directories
- Save CSVs to parquet (divided by category)

Usage example:
> python vendors/first_rate/download.py \
  --zipped_dst_dir /data/first_rate/zipped \
  --unzipped_dst_dir /data/first_rate/unzipped \
  --pq_dst_dir /data/first_rate/pq
"""

import argparse
import logging

import helpers.dbg as dbg
import vendors.first_rate.utils as fru

_LOG = logging.getLogger(__name__)

_WEBSITE = "http://firstratedata.com"
_ZIPPED_DST_DIR = "/data/first_rate/zipped/"
_UNZIPPED_DST_DIR = "/data/first_rate/unzipped/"
_PQ_DST_DIR = "/data/first_rate/pq"

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--zipped_dst_dir",
        required=False,
        action="store",
        default=_ZIPPED_DST_DIR,
        type=str,
    )
    parser.add_argument(
        "--unzipped_dst_dir",
        required=False,
        action="store",
        default=_UNZIPPED_DST_DIR,
        type=str,
    )
    parser.add_argument(
        "--pq_dst_dir",
        required=False,
        action="store",
        default=_PQ_DST_DIR,
        type=str,
    )
    parser.add_argument(
        "--max_num_files",
        action="store",
        default=None,
        type=int,
        help="Maximum number of files to be downloaded",
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
    rdd = fru.RawDataDownloader(_WEBSITE, args.zipped_dst_dir, args.max_num_files)
    rdd.execute()
    #
    mzcc = fru.MultipleZipCsvCombiner(
        args.zipped_dst_dir, rdd.path_object_dict, args.unzipped_dst_dir
    )
    mzcc.execute()
    #
    ctpc = fru.CsvToParquetConverter(
        args.unzipped_dst_dir, args.pq_dst_dir, "timestamp"
    )
    ctpc.execute()
    # TODO(Julia): We should also transfer the data on AWS. It's ok not to do
    # it for now.
    # > aws s3 cp --recursive /data/first_rate s3://default00-bucket/first_rate
