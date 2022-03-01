# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.13.7
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Description

# %% [markdown]
# The notebook implements an interface proposal for placing orders via Talos API (REST).
#
# Example:
# https://github.com/talostrading/samples/blob/master/python/rfqsample/rfqsample/rest.py

# %%
# %load_ext autoreload
# %autoreload 2

import base64
import hashlib
import hmac
import logging

import pandas as pd
import requests

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hprint as hprint
import helpers.hsql as hsql
import im_v2.im_lib_tasks as imvimlita

# %%
hdbg.init_logger(verbosity=logging.DEBUG)

_LOG = logging.getLogger(__name__)

hprint.config_notebook()


# %% [markdown]
# ## Functions

# %%
def calculate_signature(api_secret, parts):
    """
    A signature required for some types of GET and POST requests.

    Not required for historical data.
    """
    payload = "\n".join(parts)
    hash = hmac.new(
        api_secret.encode("ascii"), payload.encode("ascii"), hashlib.sha256
    )
    hash.hexdigest()
    signature = base64.urlsafe_b64encode(hash.digest()).decode()
    return signature

def timestamp_to_tz_naive_ISO_8601(timestamp: pd.Timestamp) -> str:
    """
    Transform Timestamp into a string in format accepted by Talos API.

    Example:
    2019-10-20T15:00:00.000000Z

    Note: microseconds must be included.
    """
    hdateti.dassert_is_tz_naive(timestamp)
    timestamp_iso_8601 = timestamp.isoformat(timespec="microseconds") + "Z"
    return timestamp_iso_8601

def get_orders(host: str, path: str, query: str, signature: str) -> pd.DataFrame:
    """
    Load data from given path.
    """
    headers = {"TALOS-KEY": key_talos}
    # Example of full url:
    #  https://sandbox.talostrading.com/v1/symbols/BTC-USDT/markets/binance/ohlcv/1m?startDate=2022-02-24T19:21:00.000000Z&startDate=2022-02-24T19:25:00.000000Z&limit=100
    url = f"https://{host}{path}{query}"
    r = requests.get(url=url, params={}, headers=headers)
    if r.status_code == 200:
        data = r.json()["data"]

    return pd.DataFrame(data)


# %% [markdown]
# ### Small Q&A
#
# - How to get a list of order IDs?
# - How to load all orders (not just IDs)?
# - How to post an order to buy?
# - How to post an order to sell?
# - How to find out the status of the order?
# - ** How to link the API to a wallet?

# %% [markdown]
# ### How to load orders?
# https://docs.talostrading.com/#get-an-order-rest
#
# See the header for orders - how to pass a signature?

# %%
def get_orders(host: str, path: str, query: str, signature: str) -> pd.DataFrame:
    """
    Load data from given path.
    """
    headers = {"TALOS-KEY": key_talos}
    # Example of full url:
    #  https://sandbox.talostrading.com/v1/symbols/BTC-USDT/markets/binance/ohlcv/1m?startDate=2022-02-24T19:21:00.000000Z&startDate=2022-02-24T19:25:00.000000Z&limit=100
    url = f"https://{host}{path}{query}"
    r = requests.get(url=url, params={}, headers=headers)
    if r.status_code == 200:
        data = r.json()["data"]

    return pd.DataFrame(data)
