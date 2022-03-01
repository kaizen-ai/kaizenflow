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
import datetime
import helpers.hsql as hsql
import im_v2.im_lib_tasks as imvimlita
import uuid

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
#
# `GET /v1/orders/`

# %%
def get_orders(host: str, path: str, query: str, signature: str) -> pd.DataFrame:
    """
    Load data from given path.
    """
    headers = {"TALOS-KEY": key_talos}
    # Example of full url:
    url = f"https://{host}{path}{query}"
    r = requests.get(url=url, params={}, headers=headers)
    if r.status_code == 200:
        data = r.json()["data"]

    return pd.DataFrame(data)


# %%
key_talos = "CRYEY4S913H3"
secret_talos = "***REMOVED***"

# %%
from urllib.parse import urlparse, urlencode

# %%
utc_datetime = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000000Z")
endpoint = "tal-87.sandbox.talostrading.com"
path = "/v1/orders"
parts = [
    "GET",
    utc_datetime,
    endpoint,
    path,
]
query = {"EndDate": "2022-03-01T19:30:10.000000Z"}
query_string = urlencode(query)
if query_string:
    parts.append(query_string)
signature = calculate_signature(secret_talos, parts)

headers = {
    "TALOS-KEY": key_talos,
    "TALOS-SIGN": signature,
    "TALOS-TS": utc_datetime,
}
url = f"https://{endpoint}{path}"
if query_string:
    url += "?" + query_string
r = requests.get(url=url, headers=headers)
if r.status_code == 200:
    data = r.json()
else:
    raise Exception(f"{r.status_code}: {r.text}")

# %%
data

# %%
order = {
  "type": "NewOrderSingle",
  "data": [
    {
      "ClOrdID": "bffd1c40-dcc3-4817-b61c-4185912e99fa",
      "Markets": ["coinbase"],
      "OrdType": "Limit",
      "OrderQty": "0.1000",
      "Price": "11100.00",
      "ExpectedFillQty": "0.1000",
      "ExpectedFillPrice": "11100.00",
      "Side": "Sell",
      "Strategy": "Limit",
      "Symbol": "BTC-USD",
      "TimeInForce": "GoodTillCancel",
      "TransactTime": "2019-09-17T17:46:28.000000Z",
      "SessionID": "1109RQ13KXR00",
      "CancelSessionID": "1109RQ13KXR00",
      "SubAccount": "SubAccount",
      "Group": "groupA"
    }
  ]
}

# %%
"""
Perform a signed POST request to the Talos API
@param api_key: Talos API Key
@param api_secret: Talos API Secret
@param scheme: URL scheme, e.g. https
@param endpoint: hostname to connect to, e.g. tal-1.sandbox.talostrading.com
@param path: path for request, e.g. /v1/orders
@param data: body to submit
@return response as an object
@raise an exception if the status code is not 200
"""
url = f"https://{endpoint}{path}"
utc_datetime = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000000Z")
parts = [
    "POST",
    utc_datetime,
    endpoint,
    path,
]
if query:
    query_string = urlencode(query)
    url += f"?{query_string}"
    parts.append(query_string)

if data:
    body = json.dumps(data)
    parts.append(body)

signature = calculate_signature(api_secret, parts)

headers = {
    "TALOS-KEY": api_key,
    "TALOS-SIGN": signature,
    "TALOS-TS": utc_datetime,
}

r = requests.post(url=url, data=body, headers=headers)
if r.status_code == 200:
    data = r.json()
else:
    raise Exception(f"{r.status_code}: {r.text}")


# %%
def new_cl_ord_id():
    """ Returns a new ClOrdID. """
    return str(uuid.uuid4())


# %%
new_cl_ord_id()

# %%
order_request = {
    "ClOrdID": new_cl_ord_id(),
    "Markets": ["binance"],
    "OrderQty": "1.0000",
    "Symbol": "BTC-USDT",
    "Currency": "BTC",
    "TransactTime": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000000Z"),
    "OrdType": "Limit",
    "TimeInForce": "GoodTillCancel",
    "Price": str("43481.81"),
    "Side": "Buy",
}

# %%
url = f"https://{endpoint}{path}"
utc_datetime = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000000Z")
parts = [
    "POST",
    utc_datetime,
    "tal-87.sandbox.talostrading.com",
    "/v1/orders",
]
body = json.dumps(order_request)
parts.append(body)
signature = calculate_signature(secret_talos, parts)

headers = {
    "TALOS-KEY": key_talos,
    "TALOS-SIGN": signature,
    "TALOS-TS": utc_datetime,
}

r = requests.post(url=url, data=body, headers=headers)
if r.status_code == 200:
    r.json()
else:
    Exception(f"{r.status_code}: {r.text}")

# %%
r.status_code

# %%
