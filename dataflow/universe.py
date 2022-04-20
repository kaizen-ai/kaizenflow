"""
Contain functions to manage universes of names.

Import as:

import dataflow.universe as dtfuniver
"""
import logging
import os
from typing import List, Optional, Union

import pandas as pd

import dataflow.model as dtfmod
import helpers.hdbg as hdbg
import helpers.hgit as hgit
import im_v2.common.universe as ivcu

_LOG = logging.getLogger(__name__)


# TODO(Grisha): "Move universe-related functions to `im_v2`" CmTask #1724.


# TODO(gp): Move it to a better place.
Amid = Union[int, str]


# #############################################################################
# S&P 500
# #############################################################################


def get_sp500() -> pd.DataFrame:
    """
    Return SP500 constituents as of 2010-01-01.

    :return: dataframe like

      ticker       date
    0      A 2010-01-01
    1   AAPL 2010-01-01
    2    ABC 2010-01-01
    3    ABT 2010-01-01
    4    ACE 2010-01-01
    """
    base = hgit.get_client_root(super_module=True)
    # TODO(gp): Move this file to S3.
    df = pd.read_json(os.path.join(base, "data/sp500_2010-01-01.json"))
    return df


def get_sp500_sample(n: int, seed: int) -> List[str]:
    """
    Return list of `n` random SP500 tickers.

    :param n: number of constituents to return
    :param seed: seed for random sampling
    :return: constituents as a list of tickers
    """
    sp500 = get_sp500()
    df = sp500.sample(n=n, random_state=seed)
    return df["ticker"].to_list()


# #############################################################################
# Kibot
# #############################################################################


def _get_top_n(amids: List[Amid], n: Optional[int]) -> List[Amid]:
    if n is None:
        universe = amids
    else:
        hdbg.dassert_lte(1, n)
        hdbg.dassert_lte(n, len(amids))
        universe = amids[:n]
    universe = sorted(universe)
    return universe


def _get_kibot_universe_v1(n: Optional[int]) -> List[Amid]:
    """
    Create universe of around 20 instruments.
    """
    amids = [
        "AAPL",
        "AMZN",
        "MSFT",
        "GOOG",
        "BRK.B",
        "JNJ",
        "V",
        "PG",
        "NVDA",
        "MA",
        "HD",
        "JPM",
        "UNH",
        "ADBE",
        "CRM",
        "PYPL",
        "VZ",
        "NFLX",
        "DIS",
        "INTC",
    ]
    amids = _get_top_n(amids, n)
    return amids


def _get_kibot_universe_v2(n: Optional[int]) -> List[Amid]:
    amids = [
        "AAP",
        "ABT",
        "ACN",
        "ADCT",
        "AIG",
        "AKAM",
        "AMAT",
        "ANET",
        # "AOC",
        "ASH",
        "AVP",
        "AYE",
        "BBT",
        # "BMET",
        "CAG",
        "CBS",
        "CIEN",
        "COST",
        "CRM",
        "CSCO",
        "CTAS",
        "DGX",
        "DLPH",
        "DLTR",
        "DTV",
        "EIX",
        "EQIX",
        "EVRG",
        "EXPE",
        "FRC",
        "HCA",
        "HES",
        "HON",
        "HOT",
        "HPE",
        "HPQ",
        "HSY",
        "ICE",
        "ILMN",
        "IPG",
        "JAVA",
        "JBHT",
        "JWN",
        "KEY",
        "LIFE",
        "LLTC",
        "LLY",
        "LVLT",
        "MAS",
        "MHK",
        "MHP",
        "MIL",
        "MNK",
        "MOT",
        "MXIM",
        "NEE",
        "NFX",
        "NKTR",
        "NLSN",
        "NSC",
        "NSM",
        "NUE",
        "NVLS",
        "NVR",
        "NWL",
        "ORLY",
        "PEG",
        "PKG",
        "PKI",
        "PPL",
        "PXD",
        "QLGC",
        "RDC",
        "RHI",
        "ROST",
        "RSG",
        "SHLD",
        # "SII",
        "SLG",
        "STR",
        "SWKS",
        "TEG",
        "TGT",
        "THC",
        "TIE",
        "TMO",
        "TMUS",
        "TRV",
        "UDR",
        "UNH",
        "VFC",
        "VIAV",
        "VRTX",
        "WELL",
        "WMB",
        # "WMI",
        "XEC",
        "YUM",
        "ZBRA",
        "ZMH",
    ]
    amids = _get_top_n(amids, n)
    return amids


def _get_kibot_universe_v3(n: Optional[int]) -> List[Amid]:
    """
    Use universe from file.
    """
    # Horrible hack.
    # TODO(gp): Move it to S3 and add a wrapper.
    ticker_df = pd.read_json(
        "/Users/paul/src/lemonade/data/sp500_2010-01-01.json"
    )
    skip_list = [68, 69, 72, 77, 105, 118, 120, 220, 314, 388, 400]
    selected = ticker_df.index.difference(skip_list)
    amids = ticker_df["ticker"].loc[selected].to_list()
    amids = _get_top_n(amids, n)
    return amids


# #############################################################################
# Example1
# #############################################################################


def _get_example1_universe_v1(n: Optional[int]) -> List[Amid]:
    """
    Create universe for Example1 DAG.
    """
    vendor = "example1"
    full_symbols = ivcu.get_vendor_universe(
        vendor, version="v1", as_full_symbol=True
    )
    full_symbols = _get_top_n(full_symbols, n)
    return full_symbols


# #############################################################################
# General
# #############################################################################


def get_universe(universe_str: str) -> List[Amid]:
    # E.g., universe_str == "v1_0-top100"
    universe_version, top_n = dtfmod.parse_universe_str(universe_str)
    if universe_version == "kibot_v1":
        ret = _get_kibot_universe_v1(top_n)
    elif universe_version == "kibot_v2":
        ret = _get_kibot_universe_v2(top_n)
    elif universe_version == "kibot_v3":
        ret = _get_kibot_universe_v3(top_n)
    elif universe_version == "example1_v1":
        ret = _get_example1_universe_v1(top_n)
    else:
        raise ValueError(f"Invalid universe_str='{universe_str}'")
    return ret


# #############################################################################


# def set_amid(
#    config: cconfig.Config,
#    amid_key: cconfig.Config.Key,
#    amid: Amid,
#    assume_dummy: bool = True,
# ) -> cconfig.Config:
#    """
#    Assign an instrument to a config.
#    """
#    hdbg.dassert_isinstance(config, cconfig.Config)
#    config = config.copy()
#    hdbg.dassert_in(amid_key, config)
#    if assume_dummy:
#        hdbg.dassert_eq(config.get(amid_key), cconfig.DUMMY)
#    # TODO(gp): Use IM FilePathGenerator::generate_file_path()
#    from im.kibot.data.config import S3_PREFIX
#
#    config[amid_key] = os.path.join(S3_PREFIX, "pq/sp_500_1min", amid + ".pq")
#    # config["tags"].append(ticker)
#    return config
#
#
# def build_universe_variants(
#    config: cconfig.Config, amid_key: cconfig.Config.Key, universe_str: str
# ) -> cconfig.Config:
#    """
#    Add instrument to model.
#    """
#    amids = build_universe(universe_str)
#    _LOG.debug("Universe has %d amids", len(amids))
#    configs = [set_amid(config, amid_key, amid) for amid in amids]
#    return configs
