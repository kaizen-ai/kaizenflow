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
# The notebooks performs an EDA of the given universe of assets.

# %% [markdown]
# # Imports

# %%
# %load_ext autoreload
# %autoreload 2
# %matplotlib inline

# %%
import logging

import pandas as pd

import core.config as cconfig
import core.finance.bid_ask as cfibiask
import dataflow_amp.system.Cx as dtfamsysc
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hpandas as hpandas
import helpers.hprint as hprint
import im_v2.ccxt.data.client as icdcl
import im_v2.common.universe as ivcu
import market_data.market_data_example as mdmadaex

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()


# %% [markdown]
# # Config

# %%
config = cconfig.get_config_from_env()
if config:
    # Get config from env when running the notebook via the `run_notebook.py`
    # script, e.g., in the system reconciliation flow.
    _LOG.info("Using config from env vars")
else:
    universe_version = "v7.1"
    config = {
        "universe": {
            "vendor": "CCXT",
            "mode": "trade",
            "version": universe_version,
        },
        "start_timestamp": pd.Timestamp("2023-09-01 00:00:00+00:00", tz="UTC"),
        "end_timestamp": pd.Timestamp("2023-12-31T23:59:59+00:00", tz="UTC"),
        "bid_ask_data": {
            "im_client_config": {
                "universe_version": universe_version,
                # Data currently residing in the test bucket
                "root_dir": "s3://cryptokaizen-unit-test/v3",
                "partition_mode": "by_year_month",
                "dataset": "bid_ask",
                "contract_type": "futures",
                # Data snapshot is not applicable for data version = "v3".
                "data_snapshot": "",
                "version": "v1_0_0",
                "download_universe_version": "v7",
                "tag": "resampled_1min",
                "aws_profile": "ck",
            },
            # TODO(Grisha): for some reason the current filtering mechanism filters out `asset_ids` which
            # makes it impossible to stitch the 2 market data dfs. So adding the necessary columns manually.
            "columns": cfibiask.get_bid_ask_columns_by_level(1)
            + ["asset_id", "full_symbol", "start_ts", "knowledge_timestamp"],
            "column_remap": None,
            "filter_data_mode": "assert",
        },
        "ohlcv_data": {
            "resampling_rule": "D",
        },
        "column_names": {
            "timestamp": "timestamp",
            "full_symbol_column": "full_symbol",
            "close": "close",
            "volume": "volume",
            "volume_notional": "volume_notional",
        },
        "bar_duration": "5T",
    }
    config = cconfig.Config().from_dict(config)
print(config)

# %% [markdown]
# # Universe

# %%
# Get full symbol universe.
full_symbols = ivcu.get_vendor_universe(
    config["universe"]["vendor"],
    config["universe"]["mode"],
    version=config["universe"]["version"],
    as_full_symbol=True,
)
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
# TODO(Grisha): expose DB stage.
# Get prod `MarketData`.
db_stage = "preprod"
market_data = dtfamsysc.get_Cx_RealTimeMarketData_prod_instance1(
    asset_ids, db_stage
)
# Load and resample OHLCV data.
ohlcv_data = dtfamsysc.load_and_resample_ohlcv_data(
    market_data,
    config["start_timestamp"],
    config["end_timestamp"],
    config["bar_duration"],
)
hpandas.df_to_str(ohlcv_data, num_rows=5, log_level=logging.INFO)

# %%
# Compute notional volume.
volume_notional = (
    ohlcv_data[config["column_names"]["volume"]]
    * ohlcv_data[config["column_names"]["close"]]
)
hpandas.df_to_str(volume_notional, log_level=logging.INFO)

# %%
# Compute mean daily notional volume.
mdv_notional = (
    volume_notional.resample(config["ohlcv_data"]["resampling_rule"]).sum().mean()
)
mdv_notional = mdv_notional.sort_values(ascending=False).round(2)
# Replace asset ids with full symbols.
mdv_notional.index = [
    asset_id_to_full_symbol_mapping[idx] for idx in mdv_notional.index
]
mdv_notional.name = "mdv_notional"
hpandas.df_to_str(mdv_notional, log_level=logging.INFO)

# %%
mdv_notional.plot(kind="bar", logy=True, ylabel="notional", title="MDV")

# %% [markdown]
# # Bid / ask price changes

# %%
bid_ask_im_client = icdcl.ccxt_clients.CcxtHistoricalPqByTileClient(
    **config["bid_ask_data"]["im_client_config"]
)
#
bid_ask_market_data = mdmadaex.get_HistoricalImClientMarketData_example1(
    bid_ask_im_client,
    asset_ids,
    config["bid_ask_data"]["columns"],
    config["bid_ask_data"]["column_remap"],
    filter_data_mode=config["bid_ask_data"]["filter_data_mode"],
)
bid_ask_data = bid_ask_market_data.get_data_for_interval(
    config["start_timestamp"],
    config["end_timestamp"],
    config["column_names"]["timestamp"],
    asset_ids,
)
hpandas.df_to_str(bid_ask_data, log_level=logging.INFO)

# %%
# Remove duplicates.
use_index = True
duplicate_columns = [config["column_names"]["full_symbol_column"]]
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
half_spread = 0.5 * (
    bid_ask_data["level_1.ask_price.close"]
    - bid_ask_data["level_1.bid_price.close"]
)
bid_ask_data["half_spread"] = half_spread
bid_ask_midpoint = 0.5 * (
    bid_ask_data["level_1.ask_price.close"]
    + bid_ask_data["level_1.bid_price.close"]
)
bid_ask_data["bid_ask_midpoint"] = bid_ask_midpoint
bid_ask_data["half_spread_bps"] = 1e4 * half_spread / bid_ask_midpoint
# TODO(Paul): Add more refined bid/ask vol calculations.
n_data_points = 30
bid_vol = (
    bid_ask_data.groupby("full_symbol")["level_1.bid_price.close"]
    .pct_change()
    .rolling(n_data_points)
    .std()
)
bid_ask_data["bid_vol_bps"] = 1e4 * bid_vol
ask_vol = (
    bid_ask_data.groupby("full_symbol")["level_1.ask_price.close"]
    .pct_change()
    .rolling(n_data_points)
    .std()
)
bid_ask_data["ask_vol_bps"] = 1e4 * ask_vol

# %%
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
half_spread_bps_mean.plot(
    kind="bar", logy=True, ylabel="bps", title="Half bid/ask spread"
)

# %%
bid_vol_bps_df = bid_ask_data.pivot(columns="full_symbol", values="bid_vol_bps")
hpandas.df_to_str(bid_vol_bps_df, log_level=logging.INFO)

# %%
bid_vol_bps_mean = bid_vol_bps_df.mean().sort_values().rename("bid_vol_bps_mean")
bid_vol_bps_mean.plot(kind="bar", logy=True, ylabel="bps", title="bid vol")

# %%
bid_vol_to_half_spread = bid_vol_bps_df.divide(half_spread_bps_df)
hpandas.df_to_str(bid_vol_to_half_spread, log_level=logging.INFO)

# %%
bid_vol_to_half_spread_mean = (
    bid_vol_to_half_spread.mean()
    .sort_values(ascending=False)
    .rename("bid_vol_to_half_spread_mean")
)
bid_vol_to_half_spread_mean.plot(
    kind="bar", logy=True, title="Bid vol / half spread"
)

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
