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
import datetime
import logging
import os

import core.config as cconfig
import core.plotting as coplotti
import dataflow.model as dtfmod
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hgit as hgit
import helpers.hparquet as hparque
import helpers.hprint as hprint

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %% [markdown]
# # Build the config

# %%
amp_dir = hgit.get_amp_abs_path()
dir_name = os.path.join(
    amp_dir,
    "dataflow/model/test/outcomes/Test_run_master_research_backtest_analyzer/input/tiled_results",
)
config = {
    "dir_name": dir_name,
    "start_date": datetime.date(2000, 1, 1),
    "end_date": datetime.date(2000, 1, 31),
    "asset_id_col": "asset_id",
    "pnl_resampling_frequency": "15T",
    "annotate_forecasts_kwargs": {
        "style": "longitudinal",
        "quantization": 30,
        "liquidate_at_end_of_day": False,
        "initialize_beginning_of_day_trades_to_zero": False,
        "burn_in_bars": 3,
        "compute_extended_stats": True,
        "target_dollar_risk_per_name": 1e2,
        "modulate_using_prediction_magnitude": True,
    },
    "column_names": {
        "price_col": "vwap",
        "volatility_col": "vwap.ret_0.vol",
        "prediction_col": "prediction",
    },
    "bin_annotated_portfolio_df_kwargs": {
        "proportion_of_data_per_bin": 0.2,
        "normalize_prediction_col_values": False,
    },
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
single_tile_df.columns.levels[0].to_list()

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
    asset_ids=None,
    annotate_forecasts_kwargs=config["annotate_forecasts_kwargs"].to_dict(),
    return_portfolio_df=True,
)

# %%
coplotti.plot_portfolio_stats(
    bar_metrics, freq=config["pnl_resampling_frequency"]
)

# %%
fep = dtfmod.ForecastEvaluatorFromPrices(
    **config["column_names"],
)

# %%
output_col = "pnl"
binned_df = fep.bin_annotated_portfolio_df(
    portfolio_df, output_col=output_col, **config["bin_annotated_portfolio_df_kwargs"],
)
binned_df["mean"].mean(axis=1).plot(title=output_col)

# %%
output_col = "pnl_in_bps"
binned_df = fep.bin_annotated_portfolio_df(
    portfolio_df, output_col=output_col, **config["bin_annotated_portfolio_df_kwargs"],
)
binned_df["mean"].mean(axis=1).plot(title=output_col)

# %%
output_col = "sgn_corr"
binned_df = fep.bin_annotated_portfolio_df(
    portfolio_df, output_col=output_col, **config["bin_annotated_portfolio_df_kwargs"],
)
binned_df["mean"].mean(axis=1).plot(title=output_col)

# %%
output_col = "hit_rate"
binned_df = fep.bin_annotated_portfolio_df(
    portfolio_df, output_col=output_col, **config["bin_annotated_portfolio_df_kwargs"],
)
binned_df["mean"].mean(axis=1).plot(title=output_col)

# %% [markdown]
# # Compute aggregate portfolio stats

# %%
stats_computer = dtfmod.StatsComputer()

# %%
portfolio_stats, daily_metrics = stats_computer.compute_portfolio_stats(
    bar_metrics,
    config["pnl_resampling_frequency"],
)
display(portfolio_stats)

# %%
