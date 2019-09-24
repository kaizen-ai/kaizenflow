"""
Import as:

import vendors.kibot.utils as kut
"""

import functools
import logging
import os

import numpy as np
import pandas as pd

import helpers.cache as cache
import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)

# #############################################################################
# Read data.
# #############################################################################


# TODO(gp): Add timezone or use _ET suffix.
def _read_data(file_name, nrows):
    """
    Read row data from disk and perform basic transformations such as:
    - parse dates
    - add columns
    - check for monotonic index
    """
    _LOG.info("Reading file_name='%s' nrows=%s", file_name, nrows)
    if nrows is not None:
        _LOG.warning("Reading only the first nrows=%s rows", nrows)
    dir_name = os.path.basename(os.path.dirname(file_name))
    if dir_name in (
        "All_Futures_Contracts_1min",
        "All_Futures_Continuous_Contracts_1min",
    ):
        # 1 minute data.
        df = pd.read_csv(
            file_name, header=None, parse_dates=[[0, 1]], nrows=nrows
        )
        # According to Kibot the columns are:
        #   Date,Time,Open,High,Low,Close,Volume
        df.columns = "datetime open high low close vol".split()
        df.set_index("datetime", drop=True, inplace=True)
        _LOG.debug("Add columns")
        df["time"] = [d.time() for d in df.index]
    elif dir_name == "All_Futures_Continuous_Contracts_daily":
        # Daily data.
        df = pd.read_csv(file_name, header=None, parse_dates=[0], nrows=nrows)
        df.columns = "date open high low close vol".split()
        df.set_index("date", drop=True, inplace=True)
        # TODO(gp): Turn it into datetime using EOD timestamp. Check on Kibot.
    else:
        raise ValueError(
            "Invalid dir_name='%s' in file_name='%s'" % (dir_name, file_name)
        )
    dbg.dassert(df.index.is_monotonic_increasing)
    dbg.dassert(df.index.is_unique)
    return df


MEMORY = cache.get_disk_cache()


@MEMORY.cache
def _read_data_from_disk_cache(*args, **kwargs):
    _LOG.info("args=%s kwargs=%s", str(args), str(kwargs))
    obj = _read_data(*args, **kwargs)
    return obj


@functools.lru_cache(maxsize=None)
def read_data(*args, **kwargs):
    _LOG.info("args=%s kwargs=%s", str(args), str(kwargs))
    obj = _read_data_from_disk_cache(*args, **kwargs)
    return obj


def read_data_from_config(config):
    _LOG.info("Reading data ...")
    config.check_params(["file_name"])
    return read_data(config["file_name"], config.get("nrows", None))


def read_multiple_symbol_data(symbols, file_name_template, nrows=None):
    """
    Read multiple files for the given symbols.
    :return: dict symbol -> file
    """
    dict_df = {}
    for s in symbols:
        file_name = file_name_template % s
        dict_df[s] = read_data(file_name, nrows)
    return dict_df


# #############################################################################
# Read metadata.
# #############################################################################

_KIBOT_DIRNAME = "s3://alphamatic/kibot/metadata"


# TODO(gp): I don't have a clear understanding of what the metadata means and
#  different so I call them 1, 2, 3, ... for now, waiting for a better name.
def read_metadata1():
    """
    Symbol    Link                                                Description
    JY        http://api.kibot.com/?action=download&link=151...   CONTINUOUS JAPANESE YEN CONTRACT
    JYF18    http://api.kibot.com/?action=download&link=vrv...    JAPANESE YEN JANUARY 2018
    """
    file_name = _KIBOT_DIRNAME + "/All_Futures_Contracts_1min.csv.gz"
    df = pd.read_csv(file_name, index_col=0)
    df = df.iloc[:, 1:]
    _LOG.debug("df=\n%s", df.head(3))
    _LOG.debug("df.shape=%s", df.shape)
    return df


def read_metadata2():
    """
    Symbol    Link                                                Description
    JY        http://api.kibot.com/?action=download&link=151...   CONTINUOUS JAPANESE YEN CONTRACT
    JYF18    http://api.kibot.com/?action=download&link=vrv...    JAPANESE YEN JANUARY 2018
    """
    file_name = _KIBOT_DIRNAME + "/All_Futures_Contracts_daily.csv.gz"
    df = pd.read_csv(file_name, index_col=0)
    df = df.iloc[:, 1:]
    _LOG.debug("df=\n%s", df.head(3))
    _LOG.debug("df.shape=%s", df.shape)
    return df


def read_metadata3():
    """
    SymbolBase    Symbol    StartDate    Size(MB)    Description                           Exchange
    ES            ES        9/30/2009    50610.0     CONTINUOUS E-MINI S&P 500 CONTRACT    Chicago Mercantile Exchange Mini Sized Contrac...
    ES            ESH11     4/6/2010     891.0       E-MINI S&P 500 MARCH 2011             Chicago Mercantile Exchange Mini Sized Contrac...
    """
    file_name = _KIBOT_DIRNAME + "/Futures_tickbidask.txt.gz"
    df = pd.read_csv(file_name, index_col=0, skiprows=5, header=None, sep="\t")
    df.columns = (
        "SymbolBase Symbol StartDate Size(MB) Description Exchange".split()
    )
    df.index.name = None
    _LOG.debug("df=\n%s", df.head(3))
    _LOG.debug("df.shape=%s", df.shape)
    return df


def read_metadata4():
    """
    SymbolBase Symbol  StartDate  Size(MB)    Description                                  Exchange
    JY         JY      9/27/2009  183.0       CONTINUOUS JAPANESE YEN CONTRACT             Chicago Mercantile Exchange (CME GLOBEX)
    TY         TY      9/27/2009  180.0       CONTINUOUS 10 YR US TREASURY NOTE CONTRACT   Chicago Board Of Trade (CBOT GLOBEX)
    FV         FV      9/27/2009  171.0       CONTINUOUS 5 YR US TREASURY NOTE CONTRACT    Chicago Board Of Trade (CBOT GLOBEX)
    """
    file_name = _KIBOT_DIRNAME + "/FuturesContinuous_intraday.txt.gz"
    df = pd.read_csv(file_name, index_col=0, skiprows=5, header=None, sep="\t")
    df.columns = (
        "SymbolBase Symbol StartDate Size(MB) Description Exchange".split()
    )
    df.index.name = None
    _LOG.debug("df=\n%s", df.head(3))
    _LOG.debug("df.shape=%s", df.shape)
    return df


# #############################################################################
# Transform.
# #############################################################################

# Most of the following functions transform the data in place (i.e., adding
# more data) and are idempotent. An alternative design choice would be to
# return the new data and let the client add the data.
# TODO(gp): Think about this.


def compute_ret_0(prices, mode):
    # TODO(gp): Maybe move this up or to utilities since it can be used for
    # many different places (even if it's very simple code).
    if mode == "pct_change":
        ret_0 = prices.pct_change()
    elif mode == "log_rets":
        ret_0 = np.log(prices) - np.log(prices.shift(1))
    elif mode == "diff":
        # TODO(gp): Use shifts for clarity, e.g.,
        # df["ret_0"] = df["open"] - df["open"].shift(1)
        ret_0 = prices.diff()
    else:
        raise ValueError("Invalid mode='%s'" % mode)
    ret_0.name = "ret_0"
    return ret_0


def compute_ret_0_from_daily_prices(price_df, col_name, mode):
    dbg.dassert_eq(price_df.index.name, "date")
    dbg.dassert_in(col_name, price_df.columns)
    return compute_ret_0(price_df[col_name], mode)


def compute_ret_0_from_multiple_daily_prices(price_dict_df, col_name, mode):
    dbg.dassert_isinstance(price_dict_df, dict)
    rets = []
    for s, price_df in price_dict_df.items():
        _LOG.debug("Processing s=%s", s)
        rets_tmp = compute_ret_0_from_daily_prices(price_df, col_name, mode)
        rets_tmp = pd.DataFrame(rets_tmp)
        rets_tmp.columns = ["%s_ret_0" % s]
        rets.append(rets_tmp)
    rets = pd.concat(rets, sort=True, axis=1)
    return rets


# #############################################################################

# Kibot documentation (from http://www.kibot.com/Support.aspx#data_format)
# states the following timing semantic:
#    "a time stamp of 10:00 AM is for a period between 10:00:00 AM and 10:00:59 AM"
#    "All records with a time stamp between 9:30:00 AM and 3:59:59 PM represent
#    the regular US stock market trading session."
#
# Thus the open price at time "ts" corresponds to the instantaneous price at
# time "ts", which by Alphamatic conventions corresponds to the "end" of an
# interval in the form [a, b) interval

# As a consequence our usual "ret_0" # (price entering instantaneously at time
# t - 1 and exiting at time t) is implemented in terms of Kibot data as:
#   ret_0(t) = open_price(t) - open_price(t - 1)
#
#              datetime     open     high      low    close   vol      time  ret_0
# 0 2009-09-27 18:00:00  1042.25  1043.25  1042.25  1043.00  1354  18:00:00    NaN
# 1 2009-09-27 18:01:00  1043.25  1043.50  1042.75  1042.75   778  18:01:00   1.00
#
# E.g., ret_0(18:01) is the return realized entering (instantaneously) at 18:00
# and exiting at 18:01
#
# In reality we need time to:
# - compute the forecast
# - enter the position
# We can't use open at time t - 1 since this would imply instantaneous forecast
# We can use data at time t - 2, which corresponds to [t-1, t-2), although
# still we would need to enter instantaneously A better assumption is to let 1
# minute to enter in position, so:
# - use data for [t - 2, t - 1) (which Kibot tags with t - 2)
# - enter in position between [t - 1, t)
# - capture the return realized between [t, t + 1]
# In other terms we need 1 extra delay (probably 2 would be even safer)


def compute_ret_0_from_1min_prices(price_df, mode):
    dbg.dassert_eq(price_df.index.name, "datetime")
    col_name = "open"
    dbg.dassert_in(col_name, price_df.columns)
    #
    rets = compute_ret_0(price_df[col_name], mode)
    # TODO(gp): Resample forward to 1min.
    return rets


def compute_first_causal_lag_from_1min_prices(price_df, mode):
    """
    Given the timing semantic of Kibot the first "causal" return we can use to
    trade ret_0 is the
    TODO(gp): finish this
    """
    dbg.dassert_eq(price_df.index.name, "datetime")
    dbg.dassert_in("open", price_df.columns)
    dbg.dassert_in("close", price_df.columns)
    #
    if mode == "pct_change":
        rets = price_df["close"].shift(1) - price_df["open"].shift(1)
        rets /= price_df["open"].shift(1)
    elif mode == "log_rets":
        rets = price_df["close"].shift(1) - price_df["open"].shift(1)
        rets = np.log(1 + rets)
    elif mode == "diff":
        rets = price_df["close"].shift(1) - price_df["open"].shift(1)
    else:
        raise ValueError("Invalid mode='%s'" % mode)
    rets.name = "ret_1_star"
    return rets


def compute_ret_0_from_multiple_1min_prices(price_dict_df, mode):
    dbg.dassert_isinstance(price_dict_df, dict)
    rets = []
    for s, price_df in price_dict_df.items():
        _LOG.debug("Processing s=%s", s)
        rets_tmp = compute_ret_0_from_1min_prices(price_df, mode)
        rets_tmp = pd.DataFrame(rets_tmp)
        rets_tmp.columns = ["%s_ret_0" % s]
        rets.append(rets_tmp)
    rets = pd.concat(rets, sort=True, axis=1)
    return rets
