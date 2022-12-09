# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.13.8
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Description

# %% [markdown]
# This notebook contains examples of CCXT functionality.

# %% [markdown]
# ## Imports

# %%
# %load_ext autoreload
# %autoreload 2
import logging
import pprint

import ccxt
import pandas as pd

import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hprint as hprint
import helpers.hsecrets as hsecret

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %%
# Print all exchanges.
print(len(ccxt.exchanges))
print(ccxt.exchanges)

# %%
# Create Binance.
exchange_id = "binance"
mode = "test"
contract_type = "futures"
# Select credentials for provided exchange.
if mode == "test":
    secrets_id = exchange_id + "_sandbox"
else:
    secrets_id = exchange_id
exchange_params = hsecret.get_secret(secrets_id)
# Enable rate limit.
exchange_params["rateLimit"] = True
# Log into futures/spot market.
if contract_type == "futures":
    exchange_params["options"] = {"defaultType": "future"}

# Create a CCXT Exchange class object.
ccxt_exchange = getattr(ccxt, exchange_id)
exchange = ccxt_exchange(exchange_params)
if mode == "test":
    exchange.set_sandbox_mode(True)
    _LOG.warning("Running in sandbox mode")
hdbg.dassert(
    exchange.checkRequiredCredentials(),
    msg="Required credentials not passed",
)

# %% [markdown]
# ## Exchange properties

# %%
exchange

# %%
print(ccxt.binance)

# %%
pprint.pprint(exchange.api)


# %%
def print_list(list_):
    print("num=%s" % len(list_))
    print("values=%s" % " ".join(list_))


# %%
exchange.loadMarkets()
print_list(exchange.markets.keys())
# Equivalent to:
#print_list(exchange.symbols)
#pprint.pprint(exchange.markets)

# %%
print_list(exchange.currencies)

# %% [markdown]
# ## Exchange metadata

# %%
# Print all the values.
pprint.pprint(exchange.has)

# %% [markdown]
# ## Loading markets

# %% [markdown]
# ## Symbols and Market Ids

# %%
print(exchange.load_markets())

# %%
pprint.pprint(exchange.markets['ETH/USDT'])

# %%
print(exchange.market_id('ETH/USDT'))

# %%
exchange.symbols

# %%
exchange.currencies

# %%
exchange.commonCurrencies

# %% [markdown]
# ## Implicit API methods

# %%
print(dir(ccxt.binance()))

# %% [markdown]
# # Unified API

# %%
exchange.fetchMarkets()

# %%
exchange.fetchCurrencies()

# %%
symbol = "BTC/USDT"
exchange.fetchOrderBook(symbol)

# %%
# Not supported for Binance
#exchange.fetchStatus()

# %%
exchange.fetchL2OrderBook(symbol)

# %%
exchange.fetchTrades(symbol)

# %%
exchange.fetchTicker(symbol)

# %%
exchange.fetchBalance()

# %%
## Order book

# %%
exchange.fetch_order_book(symbol)

# %% [markdown]
# ## Market price

# %%
# Python
orderbook = exchange.fetch_order_book (exchange.symbols[0])
bid = orderbook['bids'][0][0] if len (orderbook['bids']) > 0 else None
ask = orderbook['asks'][0][0] if len (orderbook['asks']) > 0 else None
spread = (ask - bid) if (bid and ask) else None
print (exchange.id, 'market price', { 'bid': bid, 'ask': ask, 'spread': spread })

# %% [markdown]
# # Private API

# %%
#exchange.fetchAccounts()

# %%
balance = exchange.fetchBalance()

balance

# %%
balance.keys()

# %%