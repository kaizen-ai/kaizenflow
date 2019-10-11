#!/usr/bin/env python
"""
Download data from kibot.com, compress each file, upload it to S3.

# Start from scratch
> futures_1mins/kibot_download.py -t All_Futures_Contracts_daily --zipped --delete_s3_dir

# Debug
> futures_1mins/kibot_download.py -t All_Futures_Contracts_daily --zipped --serial -v DEBUG
"""

import argparse
import logging
import os

import bs4
import numpy as np
import pandas as pd
import tqdm
from joblib import Parallel, delayed

import helpers.dbg as dbg
import helpers.io_ as io_
import helpers.s3 as hs3
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)

# ##############################################################################


def _extract_links(src_file):
    # Need to save from the browser because of auth issues.
    html = io_.from_file(src_file, split=False)
    soup = bs4.BeautifulSoup(html, "html.parser")
    #
    tables = soup.findAll("table")
    found = False
    for table in tables:
        if table.findParent("table") is None:
            if table.get("class", "") == ["ms-classic4-main"]:
                _LOG.info("Found table")
                df = pd.read_html(str(table))[0]
                df.columns = df.iloc[0]
                df = df.iloc[1:]
                cols = [
                    np.where(tag.has_attr("href"), tag.get("href"), "no link")
                    for tag in table.find_all("a")
                ]
                df["Link"] = [str(c) for c in cols]
                found = True
    dbg.dassert(found)
    return df


def _download(local_dir, aws_dir, row, zipped):
    aws_file = aws_dir + "/"
    if zipped:
        aws_file += "%s.csv.gz" % row["Symbol"]
    else:
        aws_file += "%s.csv" % row["Symbol"]
    # Check if S3 file exists.
    rc = si.system("aws s3 ls " + aws_file, abort_on_error=False)
    exists = not rc
    _LOG.debug("%s -> exists=%s", aws_file, exists)
    if exists:
        _LOG.info("%s -> skip", aws_file)
        return False
    # Download data.
    local_file = "%s/%s.csv" % (local_dir, row["Symbol"])
    # --compression=gzip
    cmd = "wget '%s' -O %s" % (row["Link"], local_file)
    si.system(cmd)
    if zipped:
        dst_file = local_file.replace(".csv", ".csv.gz")
        cmd = "gzip %s -c >%s" % (local_file, dst_file)
        si.system(cmd)
        # Delete csv file.
        cmd = "rm -f %s" % local_file
        si.system(cmd)
        #
        local_file = dst_file
    # Copy to s3.
    cmd = "aws s3 cp %s s3://%s" % (local_file, aws_file)
    si.system(cmd)
    # Delete local file.
    cmd = "rm -f %s" % local_file
    si.system(cmd)
    return True


def _parse():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "-z",
        "--zipped",
        action="store_true",
        help="Compress file before uploading to S3",
    )
    parser.add_argument(
        "-s", "--serial", action="store_true", help="Download data serially"
    )
    parser.add_argument(
        "--delete_s3_dir",
        action="store_true",
        help="Delete the S3 dir before starting uploading",
    )
    parser.add_argument(
        "-t",
        "--tag",
        action="store",
        choices=[
            "All_Futures_Contracts_daily",
            "All_Futures_Contracts_1min",
            "All_Futures_Continuous_Contracts_1min",
            "All_Futures_Continuous_Contracts_daily",
            "All_Futures_Continuous_Contracts_tick",
        ],
        required=True,
        help="Select the dataset to download",
    )
    parser.add_argument(
        "-v",
        dest="log_level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level",
    )
    return parser


def _main(parser):
    args = parser.parse_args()
    dbg.init_logger(verb=args.log_level, use_exec_path=True)
    #
    tag = args.tag
    # tag = "All_Futures_Contracts_daily"
    # tag = "All_Futures_Contracts_1min"
    # tag = "All_Futures_Continuous_Contracts_daily"
    # tag = "All_Futures_Continuous_Contracts_1min"
    # tag = "All_Futures_Continuous_Contracts_tick"
    #
    html_file = "futures_1mins/kibot_metadata/%s.html" % tag
    csv_file = "futures_1mins/kibot_metadata/%s.csv" % tag
    if not os.path.exists(csv_file):
        _LOG.warning("Missing %s: generating it", csv_file)
        df = _extract_links(html_file)
        df.to_csv(csv_file)
    # Read the data.
    _LOG.info("Reading data from %s", csv_file)
    df = pd.read_csv(csv_file, index_col=0)
    _LOG.info("Number of files to download: %s", df.shape[0])
    _LOG.info(df.head())
    #
    dst_dir = "./" + tag
    io_.create_dir(dst_dir, incremental=False)
    #
    aws_dir = os.path.join(hs3.get_path(), "kibot", tag)
    if args.delete_s3_dir:
        _LOG.warning("Deleting s3 file %s", aws_dir)
        cmd = "aws s3 rm --recursive %s" % aws_dir
        si.system(cmd)
    # Download data.
    if not args.serial:
        Parallel(n_jobs=5, verbose=10)(
            delayed(_download)(dst_dir, aws_dir, row, args.zipped)
            for i, row in df.iterrows()
        )
    else:
        for _, row in tqdm.tqdm(df.iterrows(), total=len(df)):
            _download(dst_dir, aws_dir, row, args.zipped)


if __name__ == "__main__":
    _main(_parse())
