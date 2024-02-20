# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.15.2
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Description

# %% [markdown]
# The notebooks performs an EDA of the v8 OHLCV data.<br>
# This notebook is branched from `im_v2/common/universe/notebooks/Master_universe_analysis.ipynb`.

# %% [markdown]
# # Imports

# %%
# %load_ext autoreload
# %autoreload 2
# %matplotlib inline

# %%
import logging

import numpy as np
import pandas as pd

import core.config as cconfig
import core.finance as cofinanc
import dataflow_amp.system.Cx as dtfamsysc
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hpandas as hpandas
import helpers.hprint as hprint
import im_v2.ccxt.data.client as icdcl
import im_v2.common.data.client.historical_pq_clients as imvcdchpcl
import im_v2.common.universe as ivcu
import market_data as mdata
import market_data.market_data_example as mdmadaex

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()


# %% [markdown]
# # Functions


# %%
def plot_standardized(data: pd.DataFrame, title: str) -> None:
    """
    Standardize data levels and plot.
    """
    data_standardized = (data - data.mean()) / data.std()
    data_standardized.plot(title=title)


# %% [markdown]
# # Config

# %%
config = cconfig.get_config_from_env()
if config:
    # Get config from env when running the notebook via the `run_notebook.py`
    # script, e.g., in the system reconciliation flow.
    _LOG.info("Using config from env vars")
else:
    universe_version = "v8"
    wall_clock_time = pd.Timestamp("2100-01-01T00:00:00+00:00")
    config = {
        "universe": {
            "vendor": "CCXT",
            "mode": "download",
            "version": universe_version,
            "as_full_symbol": True,
        },
        "ohlcv_data": {
            "start_timestamp": pd.Timestamp("2023-07-17T23:00:00+00:00"),
            "end_timestamp": pd.Timestamp("2023-09-17T23:59:00+00:00"),
            "bar_duration": "1T",
            "im_client_config": {
                "vendor": "ccxt",
                "universe_version": universe_version,
                "root_dir": "s3://cryptokaizen-data-test/v3",
                "resample_1min": False,
                "partition_mode": "by_year_month",
                "dataset": "ohlcv",
                "contract_type": "futures",
                "data_snapshot": "",
                "aws_profile": "ck",
                "version": "v1_0_0",
                "download_universe_version": "v8",
                "tag": "downloaded_1min",
                "download_mode": "bulk",
                "downloading_entity": "manual",
            },
            "market_data_config": {
                "columns": None,
                "column_remap": None,
                "wall_clock_time": wall_clock_time,
            },
            "column_names": {
                "close": "close",
                "volume": "volume",
            },
        },
        "bid_ask_data": {
            "start_timestamp": pd.Timestamp("2024-01-23T00:00:00+00:00"),
            "end_timestamp": pd.Timestamp("2024-02-16T00:00:00+00:00"),
            "im_client_config": {
                "universe_version": "v8",
                "root_dir": "s3://cryptokaizen-data-test/v3",
                "partition_mode": "by_year_month",
                "dataset": "bid_ask",
                "contract_type": "futures",
                "data_snapshot": "",
                "version": "v1_0_0",
                "download_universe_version": "v8",
                "tag": "resampled_1min",
                "aws_profile": "ck",
            },
            "market_data_config": {
                "columns": cofinanc.get_bid_ask_columns_by_level(1)
                + ["asset_id", "full_symbol", "start_ts", "knowledge_timestamp"],
                "column_remap": None,
                "wall_clock_time": wall_clock_time,
                "filter_data_mode": "assert",
            },
            "column_names": {
                "timestamp": "timestamp",
                "full_symbol_column": "full_symbol",
                "close": "close",
                "volume": "volume",
                "volume_notional": "volume_notional",
            },
        },
        "plot_kwargs": {
            "kind": "barh",
            "logx": True,
            "figsize": (20, 100),
        },
    }
    config = cconfig.Config().from_dict(config)
print(config)

# %% [markdown]
# # Universe

# %%
# Get full symbol universe.
full_symbols = ivcu.get_vendor_universe(**config["universe"])
_LOG.info("The number of coins in the universe=%s", len(full_symbols))
full_symbols

# %%
# Get asset ids.
asset_ids = [
    ivcu.string_to_numerical_id(full_symbol) for full_symbol in full_symbols
]
asset_ids

# %%
# Get asset id to full symbol mapping.
asset_id_to_full_symbol_mapping = ivcu.build_numerical_to_string_id_mapping(
    full_symbols
)
asset_id_to_full_symbol_mapping

# %% [markdown]
# # Mean daily notional volume

# %%
im_client = imvcdchpcl.HistoricalPqByCurrencyPairTileClient(
    **config["ohlcv_data"]["im_client_config"]
)
market_data = mdata.get_HistoricalImClientMarketData_example1(
    im_client,
    asset_ids,
    **config["ohlcv_data"]["market_data_config"],
)

# %%
# Load and resample OHLCV data.
ohlcv_data = dtfamsysc.load_and_resample_ohlcv_data(
    market_data,
    config["ohlcv_data"]["start_timestamp"],
    config["ohlcv_data"]["end_timestamp"],
    config["ohlcv_data"]["bar_duration"],
)
hpandas.df_to_str(ohlcv_data, num_rows=5, log_level=logging.INFO)

# %%
# Compute notional volume.
volume_notional = (
    ohlcv_data[config["ohlcv_data"]["column_names"]["volume"]]
    * ohlcv_data[config["ohlcv_data"]["column_names"]["close"]]
)
hpandas.df_to_str(volume_notional, log_level=logging.INFO)

# %%
# Compute mean daily notional volume.
mdv_notional = volume_notional.resample("D").sum().mean()
mdv_notional = mdv_notional.sort_values(ascending=False).round(2)
# Replace asset ids with full symbols.
mdv_notional.index = [
    asset_id_to_full_symbol_mapping[idx] for idx in mdv_notional.index
]
mdv_notional.name = "mdv_notional"
hpandas.df_to_str(mdv_notional, log_level=logging.INFO)

# %%
title = "MDV"
ylabel = "notional"
mdv_notional.plot(
    title=title,
    ylabel=ylabel,
    **config["plot_kwargs"],
)

# %%
ohlcv_volume = ohlcv_data[
    config["ohlcv_data"]["column_names"]["volume"]
].tz_convert("US/Eastern")
ohlcv_volume = ohlcv_volume.rename(
    columns={
        col: asset_id_to_full_symbol_mapping[col] for col in ohlcv_volume.columns
    }
)
hpandas.df_to_str(ohlcv_volume, num_rows=5, log_level=logging.INFO)

# %%
mean_hourly_volume = ohlcv_volume.groupby(lambda x: x.hour).mean()
hpandas.df_to_str(mean_hourly_volume, num_rows=5, log_level=logging.INFO)

# %%
plot_standardized(mean_hourly_volume, "Mean hourly volume")

# %%
mean_minutely_volume = ohlcv_volume.groupby(lambda x: x.minute).mean()
hpandas.df_to_str(mean_minutely_volume, num_rows=5, log_level=logging.INFO)

# %%
plot_standardized(mean_minutely_volume, "Mean minutely volume")

# %%
# Days of the week are numbered as follows:
# Mon=0, Tue=1, Wed=2, Thu=3, Fri=4, Sat=5, Sun=6.
mean_weekday_volume = ohlcv_volume.groupby(ohlcv_volume.index.weekday).mean()
hpandas.df_to_str(mean_weekday_volume, num_rows=5, log_level=logging.INFO)

# %%
plot_standardized(mean_weekday_volume, "Mean weekday volume")

# %% [markdown]
# # Bid / ask price changes

# %%
bid_ask_im_client = icdcl.ccxt_clients.CcxtHistoricalPqByTileClient(
    **config["bid_ask_data"]["im_client_config"]
)

# %%
bid_ask_market_data = mdmadaex.get_HistoricalImClientMarketData_example1(
    bid_ask_im_client,
    asset_ids,
    **config["bid_ask_data"]["market_data_config"],
)

# %%
bid_ask_data = bid_ask_market_data.get_data_for_interval(
    config["bid_ask_data"]["start_timestamp"],
    config["bid_ask_data"]["end_timestamp"],
    config["bid_ask_data"]["column_names"]["timestamp"],
    asset_ids,
)
bid_ask_data.head()

# %%
# Remove duplicates.
use_index = True
duplicate_columns = [config["bid_ask_data"]["column_names"]["full_symbol_column"]]
_LOG.info(
    "The number of rows before removing duplicates=%s", bid_ask_data.shape[0]
)
bid_ask_data = hpandas.drop_duplicates(
    bid_ask_data,
    column_subset=duplicate_columns,
    use_index=use_index,
)
_LOG.info(
    "The number of rows after removing duplicates=%s", bid_ask_data.shape[0]
)

# %%
n_data_points = 30
bid_ask_data = cofinanc.compute_bid_ask_metrics(bid_ask_data, n_data_points)
hpandas.df_to_str(bid_ask_data, log_level=logging.INFO)

# %%
half_spread_bps_df = bid_ask_data.pivot(
    columns="full_symbol", values="half_spread_bps"
)
hpandas.df_to_str(half_spread_bps_df, log_level=logging.INFO)

# %%
half_spread_bps_mean = (
    half_spread_bps_df.mean().sort_values().rename("half_spread_bps_mean")
)
#
title = "Half bid/ask spread"
ylabel = "bps"
half_spread_bps_mean.plot(
    title=title,
    ylabel=ylabel,
    **config["plot_kwargs"],
)

# %%
bid_vol_bps_df = bid_ask_data.pivot(columns="full_symbol", values="bid_vol_bps")
hpandas.df_to_str(bid_vol_bps_df, log_level=logging.INFO)

# %%
bid_vol_bps_mean = bid_vol_bps_df.mean().sort_values().rename("bid_vol_bps_mean")
#
title = "bid vol"
ylabel = "bps"
bid_vol_bps_mean.plot(
    title=title,
    ylabel=ylabel,
    **config["plot_kwargs"],
)

# %%
bid_vol_to_half_spread = bid_vol_bps_df.divide(half_spread_bps_df)
hpandas.df_to_str(bid_vol_to_half_spread, log_level=logging.INFO)

# %%
bid_vol_to_half_spread_mean = (
    bid_vol_to_half_spread.mean()
    .sort_values(ascending=False)
    .rename("bid_vol_to_half_spread_mean")
)
#
title = "Bid vol / half spread"
bid_vol_to_half_spread_mean.plot(
    title=title,
    **config["plot_kwargs"],
)

# %%
ask_vol_bps_df = bid_ask_data.pivot(columns="full_symbol", values="ask_vol_bps")
hpandas.df_to_str(ask_vol_bps_df, log_level=logging.INFO)

# %%
total_vol = np.sqrt(bid_vol_bps_df**2 + ask_vol_bps_df**2).tz_convert(
    "US/Eastern"
)
mean_hourly_total_vol = total_vol.groupby(lambda x: x.hour).mean()
hpandas.df_to_str(mean_hourly_total_vol, log_level=logging.INFO)

# %%
plot_standardized(mean_hourly_total_vol, "Mean hourly total volume")

# %%
mean_minutely_total_vol = total_vol.groupby(lambda x: x.minute).mean()
hpandas.df_to_str(mean_minutely_total_vol, log_level=logging.INFO)

# %%
plot_standardized(mean_minutely_total_vol, "Mean minutely total volume")

# %%
# Days of the week are numbered as follows:
# Mon=0, Tue=1, Wed=2, Thu=3, Fri=4, Sat=5, Sun=6.
mean_weekday_total_vol = total_vol.groupby(total_vol.index.weekday).mean()
hpandas.df_to_str(mean_weekday_total_vol, log_level=logging.INFO)

# %%
plot_standardized(mean_weekday_total_vol, "Mean weekday total volume")

# %% [markdown]
# # Compute rank correlation

# %%
liquidity_metrics_df = pd.concat(
    [
        mdv_notional,
        half_spread_bps_mean,
        bid_vol_bps_mean,
        bid_vol_to_half_spread_mean,
    ],
    axis=1,
)
liquidity_metrics_df

# %%
liquidity_metrics_df.corr(method="spearman")

# %%
liquidity_metrics_df.corr(method="kendall")

# %%
