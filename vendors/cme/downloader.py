#!/usr/bin/env python

r"""
Download product lists and contract specs from CME.

In more detail:
- download product lists table:
  https://www.cmegroup.com/trading/products/?redirect=/trading/index.html#pageNumber=1&sortAsc=false&cleared=Futures
- extract hyperlinks from product lists table
- download contract specs from each of those links
- save a dataframe with product list and contract specs for each product

Usage example:
> python vendors/cme/utils.py \
   --download_url https://www.cmegroup.com/CmeWS/mvc/ProductSlate/V1/Download.xls \
   --product_list product_list.xls \
   --product_specs list_with_specs.csv
"""

import argparse
import logging

import helpers.dbg as dbg
import vendors.cme.utils as cmeu

_LOG = logging.getLogger(__name__)

if __name__ == "__main__":
    _DOWNLOAD_URL = (
        "https://www.cmegroup.com/CmeWS/mvc/ProductSlate/V1/Download.xls"
    )
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--download_url",
        required=False,
        action="store",
        default=_DOWNLOAD_URL,
        type=str,
    )
    parser.add_argument("--product_list", required=True, action="store", type=str)
    parser.add_argument(
        "--product_specs", required=True, action="store", type=str
    )
    parser.add_argument(
        "--max_num_specs",
        action="store",
        default=None,
        type=int,
        help="Maximum number of contract specs to be downloaded",
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
    cmeu.get_list_with_specs_pipeline(
        args.download_url,
        args.product_list,
        args.product_specs,
        args.max_num_specs,
    )
