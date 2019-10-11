"""
Import as:

import vendors.etfs.utils as etfut
"""

import functools
import io
import json
import logging
import os

import numpy as np
import pandas as pd

import helpers.cache as cache
import helpers.dbg as dbg
import helpers.io_ as io_
import helpers.s3 as hs3
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)


def _read_data():
    file_name = os.path.join(
        hs3.get_path(), "etf/metadata/masterdatareports.fundamentals.csv.gz"
    )
    _LOG.debug("Loading data from %s", file_name)
    meta_df = pd.read_csv(file_name, encoding="ISO-8859-1")
    return meta_df


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


class MasterdataReports:
    @staticmethod
    def download():
        """
        We downloaded a snapshot of the data using a free account and saved it.
        This website:
            http://www.masterdatareports.com/ETFData-Sample/Fundamentals.csv
        requires a membership.
        """

    def __init__(self):
        pass

    def get_metadata(self):
        meta_df = read_data()
        #
        meta_df = self._add_family_column(meta_df)
        meta_df = self._clean(meta_df)
        #
        return meta_df

    def _add_family_column(self, meta_df):
        """
        Add a "Family" column with the ETF family (e.g., iShares) inferred from
        the name of the ETF.
        """
        valid_tags = [
            "iShares",
            "Schwab",
            "PowerShares",
            "SPDR",
            "Vanguard",
            "UBS",
        ]
        family = []
        col_name = "Composite Name"
        dbg.dassert_in(col_name, meta_df.columns)
        for row in meta_df[col_name]:
            _LOG.debug("row=%s", row)
            # Look for one of the valid tags matching the row, e.g., "Vanguard"
            # in "Vanguard Consumer Discretion ETF - DNQ".
            tag_tmp = []
            for tag in valid_tags:
                if tag in str(row):
                    tag_tmp.append(tag)
            if not tag_tmp:
                tag_tmp = "nan"
            else:
                dbg.dassert_eq(len(tag_tmp), 1, "Found too many tags=%s", tag_tmp)
                tag_tmp = tag_tmp[0]
            family.append(tag_tmp)
        meta_df["Family"] = family
        return meta_df

    def _clean(self, meta_df):
        # Clean up "Inception date".
        meta_df = meta_df.applymap(lambda x: np.nan if x == "na" else x)
        meta_df["Inception Date"] = pd.to_datetime(meta_df["Inception Date"])
        # Clean up certain columns that have a bunch of crap inside.
        col_names = ["AveVol", "NAV", "AUM", "Market Cap"]
        for col_name in col_names:
            vals = meta_df[col_name]
            vals = map(str, vals)
            new_vals = [
                float(v)
                if (
                    not v.startswith("Unable")
                    and not v.startswith("EQLT")
                    and not v.startswith("|")
                )
                else np.nan
                for v in vals
            ]
            meta_df[col_name] = new_vals
        return meta_df


# ##############################################################################

# - Attributes
#   - Commission free = Yes
#   - TODO(gp): check this
# - Issuer
#   - iShares
#   - Schwab
#   - Vanguard
# - Structure
#   - ETFs
# - Liquidity
#   - Assets > $10,000M ($10B)
#   - Avg daily volume > 1M
#   - Share > $10


def _get_json_file_name(tab):
    json_file_name = "./etfdb_data/etfdb.%s.json" % tab
    return json_file_name


# TODO(gp): The API is unreliable / broken. Probably better to use beautifulsoup.
#  A package with an API is at:
#  https://github.com/janlukasschroeder/etfdb-api
#  https://www.npmjs.com/package/etfdb-api
def download_etfdb_metadata():
    """
    Download and save all the data in one shot, since etfdb.com is flakey and
    doesn't always respond.
    """
    # https://etfdb.com/screener/#page=1&sort_by=price&sort_direction=asc&issuer=ishares&structure=ETF&assets_start=10000&average_volume_start=1000000&price_start=10
    tabs = [
        "overview",
        # "Returns",
        # "Fund-Flows",
        # "Expenses",
        # "ESG",
        # "Dividends",
        # "Risk",
        # "Holdings",
        # "Taxes",
        # "Technicals",
        # "Analysis",
        # "Realtime-Ratings"
    ]
    for tab in tabs:
        print("tab=%s" % tab)
        file_name = _get_json_file_name(tab)
        if os.path.exists(file_name):
            print("Skip")
        else:
            json_post = {
                "page": 1,
                "per_page": 1000,
                # "commission_free": "true",
                # "structure": "ETF",
                # "issuer": "ishares",
                # "sort_by": "ytd",
                # "sort_direction": "desc",
                "only": ["meta", "data"],
                "tab": tab,
            }
            io_.to_file("json_post.txt", str(json_post))
            cmd = (
                'curl -d "@json_post.txt" -X POST https://etfdb.com/api/screener/ -o %s'
                % file_name
            )
            si.system(cmd)
            #
            txt = io_.from_file(file_name, split=False)
            if "error" in txt:
                print("Error: removing %s" % file_name)
                os.remove(file_name)


def get_ishares_fundamentals():
    # file_name = "etfdb.json"
    file_name = _get_json_file_name("dummy")
    txt = io_.from_file(file_name, split=False)
    dict_ = json.loads(txt)
    # {'symbol': {'type': 'link', 'text': 'MIDU', 'url': '/etf/MIDU/'},
    #   'name': {'type': 'link',
    #    'text': 'Direxion Daily Mid Cap Bull 3X Shares',
    #    'url': '/etf/MIDU/'},
    #   'mobile_title': 'MIDU - Direxion Daily Mid Cap Bull 3X Shares',
    #   'price': '$44.01',
    #   'assets': '$54.93',
    #   'average_volume': '48,132',
    #   'ytd': '56.95%',
    #   'overall_rating': {'type': 'restricted', 'url': '/members/join/'},
    #   'asset_class': 'Equity'},
    data = dict_["data"]
    data_tmp = []
    convert_ = lambda x: float(x.replace(",", "").replace("$", ""))
    for row in data:
        data_tmp.append(
            (
                row["symbol"]["text"],
                row["name"]["text"],
                convert_(row["price"]),
                convert_(row["assets"]),
                convert_(row["average_volume"]),
                row["asset_class"],
            )
        )
    df = pd.DataFrame(
        data_tmp,
        columns="symbol descr price assets avg_volume asset_class".split(),
    )
    return df


def _get_sample_data_path(ticker):
    my_path = os.path.realpath(__file__)
    my_dir, _ = os.path.split(my_path)
    rel_path = "sample_data/%s.csv" % ticker
    path = os.path.join(my_dir, rel_path)
    dbg.dassert_exists(path, "No sample data found for ticker=%s", ticker)
    return path


def _generate_sample_data():
    """
    Reads SPY data and writes a 10-year subset.

    Column ordering is preserved. This sample data can be used to
      - Understand the raw data format
      - Provide a source of market data for unit/integration tests.

    > du -h SPY.csv
    324K    SPY.csv
    """
    ticker = "SPY"
    # Read SPY data.
    source_path = os.path.join(hs3.get_path(), "etf/data/%s.csv.gz" % ticker)
    df = pd.read_csv(source_path)
    # Convert 'Date' column to datetime and filter.
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.set_index("Date")
    df = df.loc["2007":"2016"]
    df.reset_index(inplace=True)
    # Write as csv to buffer.
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    # Write to file.
    out_path = _get_sample_data_path(ticker)
    _LOG.info("Writing to path=%s", out_path)
    io_.to_file(out_path, csv_buffer.getvalue())


def read_sample_data(ticker):
    """
    Reads sample_data/SPY.csv and performs minimal post-processing.

    :return: pd.DataFrame with DatetimeIndex
    """
    path = _get_sample_data_path(ticker)
    df = pd.read_csv(path)
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.set_index("Date")
    return df
