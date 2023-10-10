# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.15.0
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Description

# %%
# TODO(Paul): consider merging (or factoring out common code) with the `Master_research_backtest_analyzer` and `Master_execution_analysis`.

# %% [markdown]
# The notebook:
#    - Loads results of a historical simulation
#    - Computes research portfolio
#    - Saves trades and pnl to a file
#    - Performs prices and pnl cross-checks
#
# The code overlaps with that from:
#    - the `dataflow/model/notebooks/Master_research_backtest_analyzer.ipynb`: load tiled simulation, compute research pnl
#    - the `oms/notebooks/Master_execution_analysis.ipynb`: load and resmaple OHLCV prices
#
# What is really unique in the current notebook is:
#    - Converting `holdings_shares` to `target_holdings_shares`
#    - Saving data to a file
#    - Prices and pnl cross-checks

# %% [markdown]
# # Imports

# %%
# %load_ext autoreload
# %autoreload 2
# %matplotlib inline

# %%
import datetime
import logging
import os

import pandas as pd

import core.config as cconfig
import core.plotting as coplotti
import dataflow.model as dtfmod
import dataflow_amp.system.Cx as dtfamsysc
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hparquet as hparque
import helpers.hprint as hprint
import im_v2.common.universe as ivcu

# TODO(Grisha): probably `dataflow/model` should not depend on `oms`.
import oms.broker.ccxt.ccxt_utils as obccccut

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %% [markdown]
# # Build the config

# %%
market_info = obccccut.load_market_data_info()
asset_id_to_share_decimals = obccccut.subset_market_info(
    market_info, "amount_precision"
)
asset_id_to_share_decimals

# %%
config = {
    "dir_name": "/shared_data/model/historical/build_tile_configs.C5b.ccxt_v7_1-all.5T.2019-10-01_2023-07-02.ins.run0/tiled_results",
    "start_date": datetime.date(2022, 7, 2),
    "end_date": datetime.date(2023, 7, 2),
    "asset_id_col": "asset_id",
    "pnl_resampling_frequency": "D",
    "save_data_dst_dir": "/shared_data/marketing/cmtask4688",
    "annotate_forecasts_kwargs": {
        "burn_in_bars": 3,
        "style": "longitudinal",
        # Apply asset-specific rounding.
        "quantization": None,
        "target_dollar_risk_per_name": 50.0,
        "liquidate_at_end_of_day": False,
        "initialize_beginning_of_day_trades_to_zero": False,
        "asset_id_to_share_decimals": asset_id_to_share_decimals,
    },
    # TODO(Grisha): consider inferring column names from a `DagBuilder` object.
    "column_names": {
        "price_col": "vwap",
        "volatility_col": "garman_klass_vol",
        "prediction_col": "feature",
    },
    "save_data": False,
}
config = cconfig.Config().from_dict(config)
print(config)

# %% [markdown]
# # Load tiled results

# %% [markdown]
# ## Report tile stats

# %%
parquet_tile_analyzer = dtfmod.ParquetTileAnalyzer()
parquet_tile_metadata = parquet_tile_analyzer.collate_parquet_tile_metadata(
    config["dir_name"]
)

# %%
parquet_tile_analyzer.compute_metadata_stats_by_asset_id(parquet_tile_metadata)

# %%
parquet_tile_analyzer.compute_universe_size_by_time(parquet_tile_metadata)

# %%
asset_ids = parquet_tile_metadata.index.levels[0].to_list()
display(asset_ids)

# %% [markdown]
# ## Load a single-asset tile

# %%
asset_batch_size = 1
cols = None
single_asset_tile = next(
    hparque.yield_parquet_tiles_by_assets(
        config["dir_name"],
        asset_ids[0:1],
        config["asset_id_col"],
        asset_batch_size,
        cols,
    )
)

# %%
single_tile_df = dtfmod.process_parquet_read_df(
    single_asset_tile, config["asset_id_col"]
)

# %%
single_tile_df.columns.levels[0]

# %%
single_tile_df.head(3)

# %% [markdown]
# # Compute portfolio bar metrics

# %%
portfolio_df, bar_metrics = dtfmod.annotate_forecasts_by_tile(
    config["dir_name"],
    config["start_date"],
    config["end_date"],
    config["asset_id_col"],
    config["column_names"]["price_col"],
    config["column_names"]["volatility_col"],
    config["column_names"]["prediction_col"],
    annotate_forecasts_kwargs=config["annotate_forecasts_kwargs"].to_dict(),
)

# %%
portfolio_df.tail(3)

# %%
bar_metrics.tail(3)

# %%
_LOG.info("Mean GMV=%s", bar_metrics["gmv"].mean())

# %%
coplotti.plot_portfolio_stats(
    bar_metrics, freq=config["pnl_resampling_frequency"]
)

# %% [markdown]
# # Sanity check portfolio

# %%
# Check that the PnL is computed correctly by computing it in different ways
# and comparing to the reference one.
# Use the smallest correlation accross instruments to detect an error.
dtfmod.cross_check_portfolio_pnl(portfolio_df).min()

# %% [markdown]
# # Load the asset ids to full symbols mapping

# %% run_control={"marked": true}
vendor = "CCXT"
mode = "trade"
full_symbols = ivcu.get_vendor_universe(
    vendor, mode, version="v7.1", as_full_symbol=True
)
asset_id_to_full_symbol = ivcu.build_numerical_to_string_id_mapping(full_symbols)
asset_id_to_full_symbol

# %% [markdown]
# # Sanity check PnL vs target positions

# %%
universe_version = "v7.1"
vendor = "CCXT"
mode = "trade"
# Get asset ids.
asset_ids = ivcu.get_vendor_universe_as_asset_ids(universe_version, vendor, mode)
# Get prod `MarketData`.
market_data = dtfamsysc.get_Cx_RealTimeMarketData_prod_instance1(asset_ids)
# Load and resample OHLCV data.
start_timestamp = portfolio_df.index.min()
end_timestamp = portfolio_df.index.max()
_LOG.info(
    "start_timestamp=%s, end_timestamp=%s",
    start_timestamp,
    end_timestamp,
)
bar_duration = "5T"
ohlcv_df = dtfamsysc.load_and_resample_ohlcv_data(
    market_data,
    start_timestamp,
    end_timestamp,
    bar_duration,
)
# Convert to UTC to match the timezone from the research portfolio.
ohlcv_df.index = ohlcv_df.index.tz_convert("UTC")
ohlcv_df.tail(3)

# %%
# Make sure that the prices from the real-time databases match the ones
# from the research portfolio.
# TODO(Grisha): eventually understand why the correlation is not perfect,
# probably due to prices updates after the fact.
ohlcv_df["vwap"].diff().corrwith(portfolio_df["price"].diff())

# %%
# Re-compute the PnL using prices from the DB. For some reason the DB data
# starts at `2022-01-08 19:05:00-05:00` and there are small differences between
# the prices used to compute the Portfolio (Parquet data) vs the DB prices.
holdings_shares = portfolio_df["holdings_shares"].loc[
    "2022-01-08 19:05:00-05:00":
]
new_pnl = holdings_shares.shift(1).multiply(ohlcv_df["vwap"].diff())
# Check that the re-computed PnL matches the one from the research Portfolio.
new_pnl.corrwith(portfolio_df["pnl"].loc["2022-01-08 19:05:00-05:00":])

# %% [markdown]
# # Save data

# %% [markdown]
# ## Target holdings shares

# %%
idx_name = portfolio_df["holdings_shares"].index.name
target_holdings_shares = (
    portfolio_df["holdings_shares"].shift(-1).stack().reset_index()
)
# Map asset ids to fulls symbols.
target_holdings_shares["full_symbol"] = target_holdings_shares["asset_id"].apply(
    lambda x: asset_id_to_full_symbol[x]
)
# Rename the column.
target_holdings_shares = target_holdings_shares.rename(
    columns={0: "target_holdings_shares"}
)
# Keep only the relevant columns.
target_holdings_shares = target_holdings_shares[
    [idx_name, "full_symbol", "target_holdings_shares"]
]
_LOG.info("df.shape=%s", target_holdings_shares.shape)
target_holdings_shares.tail(10)

# %%
if config["save_data"]:
    target_holdings_shares_path = os.path.join(
        config["save_data_dst_dir"], "target_holdings_shares.csv.gz"
    )
    target_holdings_shares.to_csv(target_holdings_shares_path, index=False)
    tmp = pd.read_csv(target_holdings_shares_path)
    _LOG.info("df.shape=%s", tmp.shape)
    tmp.tail(10)

# %% [markdown]
# ## PnL

# %%
pnl_df = portfolio_df["pnl"].stack().reset_index()
# Mapp asset ids to fulls symbols.
pnl_df["full_symbol"] = pnl_df["asset_id"].apply(
    lambda x: asset_id_to_full_symbol[x]
)
# Rename.
pnl_df = pnl_df.rename(columns={0: "pnl"})
# Keep only the relevant columns.
pnl_df = pnl_df[["end_ts", "full_symbol", "pnl"]]
_LOG.info("df.shape=%s", pnl_df.shape)
pnl_df.tail(10)

# %%
if config["save_data"]:
    pnl_path = os.path.join(config["save_data_dst_dir"], "pnl.csv.gz")
    pnl_df.to_csv(pnl_path, index=False)
    tmp = pd.read_csv(pnl_path)
    _LOG.info("df.shape=%s", tmp.shape)
    tmp.tail(10)
