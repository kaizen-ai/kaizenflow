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
import datetime
import hashlib
import hmac
import logging
import uuid
from urllib.parse import urlencode

import pandas as pd
import requests

import helpers.hdbg as hdbg
import helpers.hprint as hprint
import helpers.hsecrets as hsecret

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
    # hdateti.dassert_is_tz_naive(timestamp)
    timestamp_iso_8601 = timestamp.isoformat(timespec="microseconds") + "Z"
    return timestamp_iso_8601


def get_orders(
    endpoint: str, path: str, public_key: str, secret_key: str
) -> pd.DataFrame:
    """
    Load data from given path.

    Loads all orders up to the moment of request
    """
    utc_datetime = datetime.datetime.utcnow().strftime(
        "%Y-%m-%dT%H:%M:%S.000000Z"
    )
    # TODO(Danya): Add time query (startDate and endDate)
    # TODO(Danya): Factor out the general form of a GET request (common with OHLCV)
    # TODO(Danya): Factor out the general part of TALOS authorization.
    # Note: some kind of query is required.
    query = {"EndDate": utc_datetime}
    query_string = urlencode(query)
    print(utc_datetime)
    get_request_parts = ["GET", utc_datetime, endpoint, path, query_string]
    signature = calculate_signature(secret_key, get_request_parts)
    # TODO(*): Get secrets from hsecrets.
    headers = {
        "TALOS-KEY": public_key,  # API public key
        "TALOS-SIGN": signature,  # an encoded secret key + request
        "TALOS-TS": utc_datetime,  # Time of request UTC.
    }
    # TODO(Danya): Factor out
    url = f"https://{endpoint}{path}?{query_string}"
    print(url)
    r = requests.get(url=url, headers=headers)
    if r.status_code == 200:
        data = r.json()
    else:
        raise Exception(f"{r.status_code}: {r.text}")
    return data


def get_talos_api_keys(mode: str = "sandbox"):
    if mode == "sandbox":
        api_keys = hsecret.get_secret("talos_sandbox")
    return api_keys


def get_cl_ord_id():
    """
    Create a ClOrdID for the POST request.
    """
    return str(uuid.uuid4())


def create_order(timestamp_ISO8601: str):
    # TODO(Danya): Add arguments: quantity, markets (exchanges), order type, etc.
    # TODO(Danya): required types of order: limit, VWAP, TWAP; TimeInForce should have "GoodUntil" passed.
    order = {
        "ClOrdID": get_cl_ord_id(),
        "Markets": ["binance"],
        "OrderQty": "1.0000",
        "Symbol": "BTC-USDT",
        "Currency": "BTC",
        "TransactTime": timestamp_ISO8601,  # Should always be the utcnow() with Talos date formatting.
        "OrdType": "Limit",
        "TimeInForce": "GoodTillCancel",
        "Price": "5.81",
        "Side": "Buy",
    }
    return order


def post_order(endpoint: str, path: str, public_key: str, secret_key: str):
    # TODO(Danya): Factor out the statement.
    utc_datetime = datetime.datetime.utcnow().strftime(
        "%Y-%m-%dT%H:%M:%S.000000Z"
    )
    parts = [
        "POST",
        utc_datetime,
        "tal-87.sandbox.talostrading.com",
        "/v1/orders",
    ]
    # TODO(Danya): Create order from outside with specific arguments
    order = create_order(utc_datetime)
    print(order)
    body = json.dumps(order)
    parts.append(body)
    # Enciode request with secret key.
    signature = calculate_signature(secret_key, parts)
    headers = {
        "TALOS-KEY": public_key,
        "TALOS-SIGN": signature,
        "TALOS-TS": utc_datetime,
    }
    # Create a POST request.
    url = f"https://{endpoint}{path}"
    r = requests.post(url=url, data=body, headers=headers)
    if r.status_code != 200:
        Exception(f"{r.status_code}: {r.text}")
    return r.status_code


# %% [markdown]
# ### Setup

# %%
# Imitation of script input parameters.
# Common elements of both GET and POST requests.
api_keys = get_talos_api_keys()
endpoint = "tal-87.sandbox.talostrading.com"  # our sandbox endpoint
path = "/v1/orders"  # path for all data related to placin orders

# %% [markdown]
# ### How to load orders?
# https://docs.talostrading.com/#get-an-order-rest

# %%
get_orders(endpoint, path, api_keys["apiKey"], api_keys["secret"])

# %% [markdown]
# ### Post an order

# %%
post_order(endpoint, path, api_keys["apiKey"], api_keys["secret"])

# %%
