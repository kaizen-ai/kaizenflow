# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.4
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %%
# %load_ext autoreload
# %autoreload 2
# %matplotlib inline

import logging
import json

import helpers.dbg as dbg
import helpers.env as henv
import helpers.io_ as io_
import helpers.printing as hprint
from typing import Any

import ccxt

# %%
dbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %%
ALL_EXCHANGES = ["binance",
"coinbase",
"kraken",
"huobi",
"ftx",
"kucoin",
"bitfinex",
"gateio",
# "binanceus" # no API access for these three exchanges.
# "bithumb"
# "bitstamp"
                ]


# %% [markdown]
# ## Functions

# %%
def log_into_exchange(exchange_id: str):
    """
    Log into exchange via ccxt.
    """
    credentials = io_.from_json("API_keys.json")
    dbg.dassert_in(exchange_id, credentials, msg="%s exchange ID not correct.")
    credentials = credentials[exchange_id]
    credentials["rateLimit"] = True
    exchange_class = getattr(ccxt, exchange_id)
    exchange = exchange_class(credentials)
    dbg.dassert(exchange.checkRequiredCredentials(), msg="Required credentials not passed.")
    return exchange

def describe_exchange_data(exchange_id: str):
    """
    """
    exchange = log_into_exchange(exchange_id)
    print("%s:" % exchange_id)
    print ("Has fetchOHLCV: %s" % exchange.has["fetchOHLCV"])
    print ("Has fetchTrades: %s" % exchange.has["fetchTrades"])
    print("Available timeframes:")
    print (exchange.timeframes)
    print("Available currency pairs:")
    print(exchange.load_markets().keys())
    print("="*50)
    return None

def download_ohlcv_data(exchange_id,
                            start_date,
                            end_date,
                            curr_symbol,
                            timeframe="1m",
                            ratelimit=1000):
    """
    Download historical data for given time period and currency.
    """
    exchange = log_into_exchange(exchange_id, mode="boss")
    dbg.dassert_in(timeframe, exchange.timeframes)
    dbg.dassert(exchange.has["fetchOHLCV"])
    dbg.dassert_in(curr_symbol, exchange.load_markets().keys())
    start_date = exchange.parse8601(start_date)
    end_date = exchange.parse8601(end_date)
    print(end_date)
    duration = exchange.parse_timeframe(timeframe) * ratelimit
    all_candles = []
    for t in range(start_date, end_date+duration, duration*ratelimit):
        print (t)
        candles = exchange.fetch_ohlcv(curr_symbol, timeframe, t, ratelimit)
        print('Fetched', len(candles), 'candles')
        if candles:
            print('From', exchange.iso8601(candles[0][0]), 'to', exchange.iso8601(candles[-1][0]))
        all_candles += candles
        total_length = len(all_candles)
        print('Fetched', total_length, 'candles in total')
    return all_candles


# %% [markdown]
# ## Check availability of historical data for exchanges

# %%
for e in ALL_EXCHANGES:
    describe_exchange_data(e)

# %% [markdown]
# ### Checking data availability at coinbase

# %%
coinbase = log_into_exchange("coinbase")

# %%
coinbase.has

# %% [markdown]
# `coinbase` exchange does not provide any kind of historical data (neither on OHLCV nor on trading orders), and it seems that its API allows only for trading.

# %% [markdown]
# ## Loading OHLCV data

# %%
