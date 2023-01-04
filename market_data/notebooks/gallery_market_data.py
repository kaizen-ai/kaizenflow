# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.14.1
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
import logging

import pandas as pd

import dataflow.system as dtfsys
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hpandas as hpandas
import helpers.hprint as hprint

# %%
hdbg.init_logger(verbosity=logging.DEBUG)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %% [markdown]
# # OHLCV market data

# %%
import pandas as pd

import im_v2.ccxt.data.client as icdcl
import im_v2.crypto_chassis.data.client as iccdc

# Initialize the client.
universe_version = "v4"
dataset = "ohlcv"
contract_type = "futures"
data_snapshot = "20220620"
im_client = icdcl.get_CcxtHistoricalPqByTileClient_example1(
    universe_version,
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
symbols = im_client.get_universe()[3:6]
display(symbols)

# %%
asset_ids = im_client.get_asset_ids_from_full_symbols(symbols)
print(asset_ids)

# %%
import market_data.market_data_example as mdmadaex

columns = None
column_remap = None

market_data = mdmadaex.get_HistoricalImClientMarketData_example1(
    im_client, asset_ids, columns, column_remap
)


# %%
# TODO(*): Document whether we need UTC.
start_ts = pd.Timestamp("2022-05-01 13:00:00+00:00")
end_ts = pd.Timestamp("2022-05-02 13:05:00+00:00")
ts_col_name = "knowledge_timestamp"

df = market_data.get_data_for_interval(
    start_ts,
    end_ts,
    ts_col_name,
    asset_ids,
)
hpandas.df_to_str(df)

# %%
data_source_node = dtfsys.HistoricalDataSource(
    "load_prices",
    market_data,
    "end_ts",
    True,
    col_names_to_remove=["knowledge_timestamp", "start_ts", "full_symbol"],
)
data_source_node.set_fit_intervals([(start_ts, end_ts)])

# %%
data = data_source_node.fit()["df_out"]
hpandas.df_to_str(data)

# %% [markdown]
# # Stitched OHLCV and bid/ask data

# %%
universe_version = "v4"
resample_1min = True
contract_type = "futures"
data_snapshot = "20220707"
#
dataset1 = "ohlcv"
im_client1 = iccdc.get_CryptoChassisHistoricalPqByTileClient_example1(
    universe_version,
    resample_1min,
    dataset1,
    contract_type,
    data_snapshot,
)
#
dataset2 = "bid_ask"
im_client2 = iccdc.get_CryptoChassisHistoricalPqByTileClient_example1(
    universe_version,
    resample_1min,
    dataset2,
    contract_type,
    data_snapshot,
)
#
asset_ids = [1467591036, 1464553467]
columns = None
column_remap = None
wall_clock_time = None
filter_data_mode = "assert"
#
im_client_market_data1 = mdmadaex.get_HistoricalImClientMarketData_example1(
    im_client1,
    asset_ids,
    columns,
    column_remap,
    wall_clock_time=wall_clock_time,
    filter_data_mode=filter_data_mode,
)
im_client_market_data2 = mdmadaex.get_HistoricalImClientMarketData_example1(
    im_client2,
    asset_ids,
    columns,
    column_remap,
    wall_clock_time=wall_clock_time,
    filter_data_mode=filter_data_mode,
)
market_data = mdmadaex.get_HorizontalStitchedMarketData_example1(
    im_client_market_data1,
    im_client_market_data2,
    asset_ids,
    columns,
    column_remap,
    wall_clock_time=wall_clock_time,
    filter_data_mode=filter_data_mode,
)

# %%
start_ts = pd.Timestamp("2022-05-01 13:00:00+00:00")
end_ts = pd.Timestamp("2022-05-02 13:05:00+00:00")
ts_col_name = "knowledge_timestamp"

df = market_data.get_data_for_interval(
    start_ts,
    end_ts,
    ts_col_name,
    asset_ids,
)
hpandas.df_to_str(df)

# %%
data_source_node = dtfsys.HistoricalDataSource(
    "load_prices",
    market_data,
    "end_ts",
    True,
    col_names_to_remove=["knowledge_timestamp", "start_ts", "full_symbol"],
)
data_source_node.set_fit_intervals([(start_ts, end_ts)])

# %%
data = data_source_node.fit()["df_out"]
hpandas.df_to_str(data)

# %%