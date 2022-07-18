# ---
# jupyter:
#   jupytext:
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

# %%
# %load_ext autoreload
# %autoreload 2
# %matplotlib inline

# %%
import datetime
import logging

import market_data as mdata
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hprint as hprint
import numpy as np
import pandas as pd
import sklearn.linear_model as sklimod
import sklearn.model_selection as skmosel

# %%
hdbg.init_logger(verbosity=logging.DEBUG)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %%
import pandas as pd
import pytest

import im_v2.ccxt.data.client as icdcl
import im_v2.common.data.client as icdc
import im_v2.common.data.client.test.im_client_test_case as icdctictc
import im_v2.crypto_chassis.data.client as iccdc


# https://github.com/cryptokaizen/cmamp/pull/2315


# Initialize the client.
universe_version = "v4"
resample_1min = False
dataset = "ohlcv"
contract_type = "futures"
data_snapshot = "20220620"
im_client = icdcl.get_CcxtHistoricalPqByTileClient_example1(
    universe_version,
    resample_1min,
    dataset,
    contract_type,
    data_snapshot,
)
# Set expected values.
full_symbols = ["binance::BTC_USDT", "binance::ADA_USDT"]
start_ts = pd.Timestamp("2022-05-01 13:00:00+00:00")
end_ts = pd.Timestamp("2022-05-01 13:05:00+00:00")

columns = None
filter_data_mode = "assert"
actual_df = im_client.read_data(
    full_symbols, start_ts, end_ts, columns, filter_data_mode
)

# %%
symbols = im_client.get_universe()[:2]

# %%
asset_ids = im_client.get_asset_ids_from_full_symbols(symbols)
print(asset_ids)

# %%
actual_df

# %%
import market_data.market_data_example as mdmadaex

#asset_ids = None
columns = None
column_remap = None

mdata = mdmadaex.get_HistoricalImClientMarketData_example1(
    im_client,
    asset_ids,
    columns,
    column_remap)


# %%
start_ts = pd.Timestamp("2022-05-01 13:00:00+00:00")
end_ts = pd.Timestamp("2022-05-01 13:05:00+00:00")
ts_col_name = "knowledge_timestamp"
        
df = mdata.get_data_for_interval(
    start_ts,
    end_ts,
    ts_col_name,
    asset_ids,
)

# %%
df
