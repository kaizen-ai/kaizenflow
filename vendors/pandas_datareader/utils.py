#!/usr/bin/env python
"""
Import as:

import vendors.pandas_datareader.utils as pdut


> amp/vendors/pandas_datareader/utils.py download_yahoo
"""

import argparse
import functools
import logging
import os

import pandas as pd
import pandas_datareader
from tqdm.autonotebook import tqdm

import helpers.cache as cache
import helpers.dbg as dbg
import helpers.io_ as io_
import helpers.parser as prsr
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)

# ##############################################################################


def _read_data(ticker):
    file_name = os.path.join(_S3_DIR, ticker) + ".csv.gz"
    _LOG.debug("Loading data from %s", file_name)
    df = pd.read_csv(
        file_name + ".gz", index_col=0, parse_dates=True, compression="gzip"
    )
    return df


MEMORY = cache.get_disk_cache()


@MEMORY.cache
def _read_data_from_disk_cache(*args, **kwargs):
    _LOG.info("args=%s kwargs=%s", str(*args), str(**kwargs))
    obj = _read_data(*args, **kwargs)
    return obj


@functools.lru_cache(maxsize=None)
def read_data(*args, **kwargs):
    _LOG.info("args=%s kwargs=%s", str(*args), str(**kwargs))
    obj = _read_data_from_disk_cache(*args, **kwargs)
    return obj


_S3_DIR = "s3://alphamatic/etf/data"


class YahooDailyQuotes:

    def __init__(self, dst_dir=None):
        if not dst_dir:
            dst_dir = "./tmp.yahoo_historical_data"
        self._dst_dir = dst_dir

    def download_historical_data(self, tickers, incremental):
        _LOG.debug("incremental=%s", incremental)
        io_.create_dir(self._dst_dir, incremental=incremental, ask_to_delete=True)
        start_date_default = "1990-01-01"
        start_dates = {}
        for ticker in tqdm(tickers):
            file_name = os.path.join(self._dst_dir, ticker) + ".csv.gz"
            if not os.path.exists(file_name):
                start_date = start_dates.get(ticker, start_date_default)
                data = pandas_datareader.data.get_data_yahoo(
                    ticker, start_date, interval="d"
                )
                data.to_csv(file_name, compression="gzip")

    def upload_historical_data_to_s3(self):
        dbg.dassert_exists(self._dst_dir)
        _LOG.info("Copying dir='%s' to '%s", self._dst_dir, _S3_DIR)
        # Copy to s3.
        cmd = "aws s3 cp --recursive %s %s" % (self._dst_dir, _S3_DIR)
        si.system(cmd, suppress_output=False)
        #
        cmd = "aws s3 ls %s" % _S3_DIR
        si.system(cmd, suppress_output=False)

    def get_data(self, ticker):
        return read_data(ticker)

    def get_multiple_data(self, field, tickers):
        data = []
        for ticker in tickers:
            df = self.get_data(ticker)
            dbg.dassert_in(field, df.columns)
            df_tmp = df[[field]]
            df_tmp.columns = [ticker]
            data.append(df_tmp)
        data = pd.concat(data, axis=1)
        return data


def get_liquid_etf_tickers():
    # In order of liquidity.
    tickers = [
        "SPY",
        "IVV",
        "VTI",
        "QQQ",
        "VWO",
        "EFA",
        "AGG",
        "IJH",
        "VTV",
        "IWM",
        "IJR",
        "IWF",
        "VUG",
        "IWD",
        "EEM",
        "VNQ",
        "LQD",
        "GLD",
        "VO",
        "VB",
        "XLF",
        "IVW",
        "TIP",
        "DIA",
        "MDY",
        "XLK",
        "SHY",
        "VGT",
        "IWB",
        "ITOT",
        "XLV",
        "IWR",
        "SDY",
        "DVY",
        "RSP",
        "IVE",
        "EWJ",
        "VV",
        "XLE",
        "VGK",
        "VBR",
        "XLY",
        "IEF",
        "IAU",
        "IWS",
        "XLP",
        "XLI",
        "TLT",
        "IWP",
        "XLU",
        "IWN",
        "IWV",
        "IWO",
        "VHT",
        "VBK",
        "EWZ",
        "IBB",
        "IJK",
        "VFH",
        "EZU",
        "VXF",
        "IJJ",
        "FXI",
        "IJS",
        "IUSG",
        "EFV",
        "IJT",
        "FVD",
        "PRF",
        "IUSV",
        "EWY",
        "OEF",
        "VDC",
        "SPYG",
        "IYR",
        "XLB",
        "IYW",
        "VPL",
        "VDE",
        "EFG",
        "VPU",
        "VIS",
        "EWT",
        "SPTM",
        "EWH",
        "RWR",
        "IXN",
        "VCR",
        "IGV",
        "SPYV",
        "EWC",
        "EWG",
        "EPP",
        "FEZ",
        "EWU",
        "IYH",
        "SPLG",
        "KBE",
        "ICF",
    ]
    return tickers


# ##############################################################################


def _download_yahoo_daily_quotes(args):
    ydq = YahooDailyQuotes()
    # tickers = get_liquid_etf_tickers()
    # ydq.download_historical_data(tickers, args.incremental)
    ydq.upload_historical_data_to_s3()


def _parse():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("positional", nargs=1, choices=["download_yahoo"])
    parser = prsr.add_bool_arg(parser, "incremental", default=True)
    parser = prsr.add_verbosity_arg(parser)
    return parser


def _main(parser):
    args = parser.parse_args()
    dbg.init_logger(verb=args.log_level, use_exec_path=True)
    action = args.positional[0]
    if action == "download_yahoo":
        _download_yahoo_daily_quotes(args)
    else:
        dbg.dfatal("Invalid action='%s'" % action)


if __name__ == "__main__":
    _main(_parse())
