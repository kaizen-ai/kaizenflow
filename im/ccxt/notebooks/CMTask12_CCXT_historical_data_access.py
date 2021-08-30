# ---
# jupyter:
#   jupytext:
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
# !pip install ccxt

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
# "binanceus" # no API for these three exchanges.
# "bithumb"
# "bitstamp"
                ]


# %% [markdown]
# ## Functions

# %%
def log_into_exchange(exchange_id: str,
                     mode: str):
    credentials = io_.from_json("API_keys.json")
    dbg.dassert_in(exchange_id, credentials, msg="%s exchange ID not correct.")
    credentials = credentials[exchange_id]
    exchange_class = getattr(ccxt, exchange_id)
    exchange = exchange_class(credentials)
    if mode == "sandbox":
        exchange.set_sandbox_mode(True)
    return exchange


# %% [markdown]
# ## Log into Binance exchange

# %% [markdown]
# Ideally 1 min last price (traded), bid / ask, traded volume
# mid-price (avg btw bid and ask) is better than last price

# %%
binance = log_into_exchange("binance", "sandbox")
