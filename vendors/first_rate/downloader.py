#!/usr/bin/env python

r"""
Download equity data from the http://firstratedata.com.

Usage example:
> python oil/utils/Task274_download_equities_form_firstrate.py \
  --dst_dir /data/firstrate/ \
  --website http://firstratedata.com
"""

import argparse
import logging

import helpers.dbg as dbg
import vendors.first_rate.utils as fru

_LOG = logging.getLogger(__name__)

_WEBSITE = "http://firstratedata.com"
_ZIPPED_DST_DIR = "/data/firstrate_zipped/"
_UNZIPPED_DST_DIR = "/data/firstrate_unzipped/"
_PQ_DST_DIR = "data/firstrate_pq"

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
        "--website", required=False, action="store", default=_WEBSITE, type=str
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

    rdd = fru.RawDataDownloader(args.website, args.zipped_dst_dir)
    rdd.execute()

    mzcc = fru.MultipleZipCSVCombiner(
        args.zipped_dst_dir, rdd.path_object_dict, args.unzipped_dst_dir
    )
    mzcc.execute()

    ctpc = fru.CSVToParquetConverter(args.unzipped_dst_dir, args.pq_dst_dir)
    ctpc.execute()
