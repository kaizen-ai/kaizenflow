# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
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
# Notebook implements interface proposal for downloading OHLCV data based on REST API

# %% [markdown]
# # Imports

# %% run_control={"marked": false}
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
# # Functions

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


def build_talos_ohlcv_path(
    currency_pair: str, exchange: str, resolution: str = "1m"
):
    """
    Get data path for given symbol and exchange.

    Example: /v1/symbols/BTC-USD/markets/coinbase/ohlcv/1h
    """
    currency_pair = currency_pair.replace("_", "-")
    data_path = (
        f"/v1/symbols/{currency_pair}/markets/{exchange}/ohlcv/{resolution}"
    )
    return data_path


def timestamp_to_tz_naive_ISO_8601(timestamp: pd.Timestamp) -> str:
    """
    Transform Timestamp into a string in format accepted by Talos API.

    Example:
    2019-10-20T15:00:00.000000Z
    2022-02-28T14:38:53.000000Z

    Note: microseconds must be included.
    """
    hdateti.dassert_is_tz_naive(timestamp)
    timestamp_iso_8601 = timestamp.isoformat(timespec="microseconds") + "Z"
    return timestamp_iso_8601


def build_talos_query(
    start_timestamp: pd.Timestamp, end_timestamp: pd.Timestamp, limit: int = 100
):
    """
    Build a query for a GET request.

    Example:
    ?startDate=2019-10-20T15:00:00.000000Z&endDate=2019-10-23:28:0.000000Z&limit=100
    """
    # TODO(Danya): Note: end timestamp is NOT included.
    query = "?"
    # Start
    if start_timestamp:
        start_date = timestamp_to_tz_naive_ISO_8601(start_timestamp)
        query += f"startDate={start_date}&"
    if end_timestamp:
        end_date = timestamp_to_tz_naive_ISO_8601(end_timestamp)
        query += f"endDate={end_date}&"
    query += f"limit={limit}"
    return query


def load_data(host: str, path: str, query: str) -> pd.DataFrame:
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
# # Downloading

# %% [markdown]
# Examples for downloading OHLCV data using Talos' REST API is taken from [here](https://docs.talostrading.com/#historical-ohlcv-candlesticks-rest).

# %%
# Imitating input parameters from a script.
# TODO(Danya): Add key and secret for sandbox to hsecrets
key_talos = "CRYEY4S913H3"
# Timestamps in the Airflow format.
end_timestamp = pd.Timestamp("2022-02-24T19:25:00")
start_timestamp = pd.Timestamp("2022-02-24T19:21:00")
# The host probably be a switch: sandbox/prod.
host = "sandbox.talostrading.com"
currency_pair = "BTC_USDT"
exchange = "binance"
resolution = "1m"
limit = 100

# %%
# Construct data path.
path = build_talos_ohlcv_path(currency_pair, exchange, resolution=resolution)
print(path)

# %%
# Construct query.
query = build_talos_query(start_timestamp, end_timestamp, limit=limit)
print(query)

# %%
# Get data as the dataframe.
load_data(host, path, query)

# %% [markdown]
# ## Comparing to DB data

# %%
env_file = imvimlita.get_db_env_path("dev")
connection_params = hsql.get_connection_info_from_env_file(env_file)
connection = hsql.get_connection(*connection_params)

# %% run_control={"marked": true}
unix_start_timestamp = hdateti.convert_timestamp_to_unix_epoch(start_timestamp)
unix_end_timestamp = hdateti.convert_timestamp_to_unix_epoch(end_timestamp)
query = (
    f"SELECT * FROM ccxt_ohlcv WHERE timestamp >='{unix_start_timestamp}'"
    f" AND timestamp <= '{unix_end_timestamp}' AND currency_pair='{currency_pair}'"
)
rt_data = hsql.execute_query_to_df(connection, query)
rt_data

# %% [markdown]
# #### Comment

# %% [markdown]
# Some important interface takeaways:
#
# - The timestamp format is tricky (trailing "Z", tz-naive, compulsory trailing .0's to microsecond level)
# - End timestamp is not included into the time range
# - Data looks consistent (at the first sight) with RT data from CCXT, although miniscule discrepancies in values of "open" should be investigated.

# %%