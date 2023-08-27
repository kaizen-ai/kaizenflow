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

import core.config as cconfig
import core.finance as cofinanc
import dataflow.system as dtfsys
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hpandas as hpandas
import helpers.hprint as hprint
import im_v2.ccxt.data.client as icdcl
import im_v2.common.data.client as icdc
import im_v2.common.universe as ivcu
import im_v2.crypto_chassis.data.client as iccdc
import market_data.market_data_example as mdmadaex

# %%
log_level = logging.INFO
hdbg.init_logger(verbosity=log_level)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %% [markdown]
# # OHLCV market data

# %%
# Initialize the client.
data_version = "v2"
universe_version = "v4"
dataset = "ohlcv"
contract_type = "futures"
data_snapshot = "20220620"
im_client = icdcl.get_CcxtHistoricalPqByTileClient_example1(
    data_version,
    universe_version,
    dataset,
    contract_type,
    data_snapshot=data_snapshot,
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
hpandas.df_to_str(df, log_level=log_level)

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
hpandas.df_to_str(data, log_level=log_level)

# %% [markdown]
# # Stitched OHLCV and bid/ask data

# %% [markdown]
# ## Data version is "v2"

# %%
universe_version = "v4"
contract_type = "futures"
data_snapshot = "20220707"
#
dataset1 = "ohlcv"
im_client1 = iccdc.get_CryptoChassisHistoricalPqByTileClient_example1(
    universe_version,
    dataset1,
    contract_type,
    data_snapshot,
)
#
dataset2 = "bid_ask"
im_client2 = iccdc.get_CryptoChassisHistoricalPqByTileClient_example1(
    universe_version,
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
hpandas.df_to_str(df, log_level=log_level)

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
hpandas.df_to_str(data, log_level=log_level)

# %% [markdown]
# ## Data version is "v3"

# %% [markdown]
# ### Config

# %%
config = {
    # TODO(Grisha): pick the correct bid/ask dataset, see
    # CmTask4778 "Choose bid-ask dataset".
    "signature": "periodic_daily.airflow.downloaded_1sec.parquet.bid_ask.futures.v3.crypto_chassis.binance.v1_0_0",
    "start_timestamp": pd.Timestamp("2023-01-01 04:10:00", tz="UTC"),
    "end_timestamp": pd.Timestamp("2023-01-01 04:13:00", tz="UTC"),
    "full_symbols": ["binance::ETH_USDT"],
    "columns": {
        "raw_data_columns": [
            "bid_price_l1",
            "ask_price_l1",
            "bid_size_l1",
            "ask_size_l1",
            "exchange_id",
            "currency_pair",
        ],
        "bid_ask_cols": ["bid_size", "ask_size", "bid_price", "ask_price"],
        "column_remap": {
            "bid_price_l1": "bid_price",
            "ask_price_l1": "ask_price",
            "bid_size_l1": "bid_size",
            "ask_size_l1": "ask_size",
        },
        "ts_col_name": "timestamp",
        "full_symbol": "full_symbol",
        "currency_pair": "currency_pair",
    },
    "resample": {
        "rule": "T",
        "vwap_groups": [],
        "resample_kwargs": {
            "closed": "right",
            "label": "right",
        },
    },
    "ohlcv_im_client_config": {
        "data_version": "v3",
        "universe_version": "v7",
        "dataset": "ohlcv",
        "contract_type": "futures",
        # Data snapshot is not applicable for data version = "v3".
        "data_snapshot": "",
    },
}
config = cconfig.Config().from_dict(config)
print(config)

# %% [markdown]
# ### Load raw data

# %%
reader = icdc.RawDataReader(config["signature"])
raw_data = reader.read_data(config["start_timestamp"], config["end_timestamp"])
raw_data.head()

# %%
data = raw_data[config["columns"]["raw_data_columns"]].rename(
    columns=config["columns"]["column_remap"].to_dict()
)
data

# %% [markdown]
# ### Resample

# %%
# TODO(Grisha): Ideally load already resampled to 1 minute bid-ask data,
# see CmTask4792 "Confirm bid / ask resampling".

# %%
col_filter = lambda col: any(
    [x in col for x in config["columns"]["bid_ask_cols"]]
)
cols_dict = {col: col for col in filter(col_filter, data.columns)}
resampling_groups = [(cols_dict, "mean", {})]
data_resampled = []
for currency_pair in data[config["columns"]["currency_pair"]].unique():
    data_single = data[data[config["columns"]["currency_pair"]] == currency_pair]
    data_resampled_single = cofinanc.resample_bars(
        data_single, **config["resample"], resampling_groups=resampling_groups
    )
    data_resampled_single[config["columns"]["currency_pair"]] = currency_pair
    data_resampled.append(data_resampled_single)
data_resampled = pd.concat(data_resampled)
data_resampled.head(3)

# %%
data_resampled["exchange"] = "binance"
# Add a full symbol column.
data_resampled[config["columns"]["full_symbol"]] = ivcu.build_full_symbol(
    data_resampled["exchange"], data_resampled[config["columns"]["currency_pair"]]
)
data_resampled.head(3)

# %% [markdown]
# ### Stiched market data

# %%
# Initialize `DataFrameImClient`.
# TODO(Grisha): Check if we can use `HistoricalPqByTileClient`,
# see CmTask4791 "Check if we can load 1 minute bid-ask data using ImClient".
bid_ask_im_client = icdc.DataFrameImClient(
    data_resampled, universe=config["full_symbols"]
)
#
filter_data_mode = "assert"
columns = None
bid_ask_im_client.read_data(
    config["full_symbols"],
    config["start_timestamp"],
    config["end_timestamp"],
    columns=columns,
    filter_data_mode=filter_data_mode,
)

# %%
# Initialize `CcxtHistoricalPqByTileClient`.
ohlcv_im_client = icdcl.get_CcxtHistoricalPqByTileClient_example1(
    **config["ohlcv_im_client_config"]
)
#
asset_ids = list(
    ivcu.build_numerical_to_string_id_mapping(config["full_symbols"]).keys()
)
column_remap = None
wall_clock_time = None
im_client_market_data1 = mdmadaex.get_HistoricalImClientMarketData_example1(
    bid_ask_im_client,
    asset_ids,
    columns,
    column_remap,
    wall_clock_time=wall_clock_time,
)
im_client_market_data2 = mdmadaex.get_HistoricalImClientMarketData_example1(
    ohlcv_im_client,
    asset_ids,
    columns,
    column_remap,
    wall_clock_time=wall_clock_time,
    filter_data_mode=filter_data_mode,
)
stiched_mdata = mdmadaex.get_HorizontalStitchedMarketData_example1(
    im_client_market_data1,
    im_client_market_data2,
    asset_ids,
    columns,
    column_remap,
    wall_clock_time=wall_clock_time,
    filter_data_mode=filter_data_mode,
)

# %%
stiched_mdata.get_data_for_interval(
    config["start_timestamp"],
    config["end_timestamp"],
    config["columns"]["ts_col_name"],
    asset_ids,
)

# %%
nid = "read_data"
ts_col_name = "end_ts"
multiindex_output = True
data_source_node = dtfsys.HistoricalDataSource(
    nid,
    stiched_mdata,
    ts_col_name,
    multiindex_output,
    col_names_to_remove=["knowledge_timestamp", "start_ts", "full_symbol"],
)
data_source_node.set_fit_intervals(
    [(config["start_timestamp"], config["end_timestamp"])]
)
df = data_source_node.fit()["df_out"]
df.head()
