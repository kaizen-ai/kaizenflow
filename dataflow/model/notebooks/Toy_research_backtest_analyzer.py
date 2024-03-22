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
# Explore research backtest results.
#
# **Note**: use `dataflow/model/notebooks/Master_backtest_analysis_param_sweep.ipynb` for standard backtest analysis.
# This notebook is used for free-form analysis and hypotheses testing, and thus is not as strictly maintained.
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
from typing import Dict

import pandas as pd

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
# # Functions


# %%
# TODO(Dan): Move to a lib.
def build_research_backtest_analyzer_config_dict(
    default_config: cconfig.Config,
) -> Dict[str, cconfig.Config]:
    """
    Build a dict of configs to run a backtest analysis.
    """
    if "sweep_param" in default_config:
        hdbg.dassert_isinstance(default_config["sweep_param"], cconfig.Config)
        # Set param values to sweep and corressponding config keys.
        sweep_param_keys = default_config["sweep_param", "keys"]
        hdbg.dassert_isinstance(sweep_param_keys, tuple)
        sweep_param_values = default_config["sweep_param", "values"]
        hdbg.dassert_isinstance(sweep_param_values, tuple)
        # Build config dict.
        config_dict = {}
        for val in sweep_param_values:
            # Update new config value.
            config = default_config.copy()
            config.update_mode = "overwrite"
            config[sweep_param_keys] = val
            config.update_mode = "assert_on_overwrite"
            # Set updated config key for config dict.
            config_dict_key = ":".join(sweep_param_keys)
            config_dict_key = " = ".join([config_dict_key, str(val)])
            # Add new config to the config dict.
            config_dict[config_dict_key] = config
    else:
        # Put single input config to a dict.
        config_dict = {"default_config": default_config}
    return config_dict


# %% [markdown]
# # Build the config dict

# %%
# Get config from env when running the notebook via the `run_notebook.py` script.
default_config = cconfig.get_config_from_env()
if default_config:
    _LOG.info("Using config from env vars")
else:
    _LOG.info("Using hardwired config")
    # Build default config.
    amp_dir = hgit.get_amp_abs_path()
    dir_name = os.path.join(
        amp_dir,
        "dataflow/model/test/outcomes/Test_run_master_research_backtest_analyzer/input/tiled_results",
    )
    default_config_dict = {
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
        "load_all_tiles_in_memory": False,
    }
    default_config = cconfig.Config().from_dict(default_config_dict)
print(default_config)

# %%
config_dict = build_research_backtest_analyzer_config_dict(default_config)
print(config_dict.keys())

# %% [markdown]
# # Load tiled results

# %% [markdown]
# ## Report tile stats

# %%
parquet_tile_analyzer = dtfmod.ParquetTileAnalyzer()
parquet_tile_metadata = parquet_tile_analyzer.collate_parquet_tile_metadata(
    default_config["dir_name"]
)

# %%
parquet_tile_analyzer.compute_metadata_stats_by_asset_id(parquet_tile_metadata)

# %%
parquet_tile_analyzer.compute_universe_size_by_time(parquet_tile_metadata)

# %%
asset_ids = parquet_tile_metadata.index.levels[0].to_list()
display(asset_ids)

# %% [markdown]
# ## Load tile data

# %%
if default_config["load_all_tiles_in_memory"]:
    asset_ids_to_load = asset_ids
else:
    asset_ids_to_load = asset_ids[0:1]
asset_batch_size = len(asset_ids_to_load)
cols = None
#
asset_tile = next(
    hparque.yield_parquet_tiles_by_assets(
        default_config["dir_name"],
        asset_ids_to_load,
        default_config["asset_id_col"],
        asset_batch_size,
        cols,
    )
)

# %%
tile_df = dtfmod.process_parquet_read_df(
    asset_tile, default_config["asset_id_col"]
)

# %%
tile_df.columns.levels[0].to_list()

# %%
tile_df.head(3)

# %% [markdown]
# # Compute portfolio bar metrics

# %%
portfolio_df_dict = {}
bar_metrics_dict = {}
for key, config in config_dict.items():
    if config["load_all_tiles_in_memory"]:
        fep = dtfmod.ForecastEvaluatorFromPrices(
            **config["column_names"].to_dict()
        )
        portfolio_df, bar_metrics = fep.annotate_forecasts(
            tile_df,
            **config["annotate_forecasts_kwargs"].to_dict(),
        )
    else:
        portfolio_df, bar_metrics = dtfmod.annotate_forecasts_by_tile(
            config["dir_name"],
            config["start_date"],
            config["end_date"],
            config["asset_id_col"],
            config["column_names"]["price_col"],
            config["column_names"]["volatility_col"],
            config["column_names"]["prediction_col"],
            asset_ids=None,
            annotate_forecasts_kwargs=config[
                "annotate_forecasts_kwargs"
            ].to_dict(),
            return_portfolio_df=True,
        )
    portfolio_df_dict[key] = portfolio_df
    bar_metrics_dict[key] = bar_metrics
portfolio_stats_df = pd.concat(bar_metrics_dict, axis=1)

# %%
coplotti.plot_portfolio_stats(
    portfolio_stats_df, freq=default_config["pnl_resampling_frequency"]
)

# %%
coplotti.plot_portfolio_binned_stats(
    portfolio_df_dict,
    **config["bin_annotated_portfolio_df_kwargs"],
)

# %% [markdown]
# # Compute aggregate portfolio stats

# %%
stats_computer = dtfmod.StatsComputer()

# %%
portfolio_stats, daily_metrics = stats_computer.compute_portfolio_stats(
    portfolio_stats_df,
    default_config["pnl_resampling_frequency"],
)
display(portfolio_stats)

# %%
