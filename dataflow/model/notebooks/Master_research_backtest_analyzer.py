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

import pandas as pd

import core.config as cconfig
import core.finance as cofinanc
import core.plotting as coplotti
import dataflow.model as dtfmod
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hgit as hgit
import helpers.hparquet as hparque
import helpers.hprint as hprint
import helpers.hsql as hsql

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
    "end_date": datetime.date(2000, 1, 2),
    "asset_id_col": "asset_id",
    "pnl_resampling_frequency": "5T",
    "annotate_forecasts_kwargs": {
        # "bulk_frac_to_remove": 0.0,
        # "bulk_fill_method": "zero",
        # "target_gmv": 1e6,
        # "dollar_neutrality": "gaussian_rank",
        "quantization": "nearest_share",
        "burn_in_bars": 3,
        "style": "longitudinal",
    },
    "regress_config": {
        "target_col": "prediction",
        # "feature_cols": [1, 2, 3, 4, 5, 6, "prediction"],
        "feature_cols": ["vwap.ret_0.vol_adj"],
        "feature_lag": 2,
        "batch_size": 50,
    },
    # TODO(Grisha): consider inferring column names from a `DagBuilder` object.
    "column_names": {
        "price_col": "vwap",
        "volatility_col": "vwap.ret_0.vol",
        "prediction_col": "feature1",
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
single_tile_df.columns.levels[0]

# %%
single_tile_df.head(3)

# %% [markdown]
# # Compute portfolio bar metrics

# %%
fep = dtfmod.ForecastEvaluatorFromPrices(
    **config["column_names"],
)

# %%
# Create backtest dataframe tile iterator.
backtest_df_iter = dtfmod.yield_processed_parquet_tiles_by_year(
    config["dir_name"],
    config["start_date"],
    config["end_date"],
    config["asset_id_col"],
    data_cols=fep.get_cols(),
    asset_ids=None,
)
# Process the dataframes in the interator.
bar_metrics = []
for df in backtest_df_iter:
    _, bar_metrics_slice = fep.annotate_forecasts(
        df,
        # bulk_frac_to_remove=fep_config["bulk_frac_to_remove"],
        # bulk_fill_method=fep_config["bulk_fill_method"],
        # target_gmv=fep_config["target_gmv"],
        **config["annotate_forecasts_kwargs"],
    )
    bar_metrics.append(bar_metrics_slice)
bar_metrics = pd.concat(bar_metrics)

# %%
coplotti.plot_portfolio_stats(
    bar_metrics, freq=config["pnl_resampling_frequency"]
)

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

# %% [markdown]
# # Overnight returns

# %%
# TODO(Grisha): consider enabling.
if False:
    # TODO(Grisha): pass params via config.
    host = ""
    dbname = ""
    port = 1000
    user = ""
    password = ""
    table_name = ""
    connection = hsql.get_connection(host, dbname, port, user, password)
    query_results = cofinanc.query_by_assets_and_dates(
        connection,
        table_name,
        asset_ids=asset_ids,
        asset_id_col=config["asset_id_col"],
        start_date=config["start_date"],
        end_date=config["end_date"],
        date_col="date",
        select_cols=[
            "date",
            "open_",
            "close",
            "total_return",
            "prev_total_return",
        ],
    )
    overnight_returns = cofinanc.compute_overnight_returns(
        query_results,
        config["asset_id_col"],
    )

# %% [markdown]
# # Regression analysis

# %%
# TODO(Grisha): consider enabling.
if False:
    coefficients, corr = dtfmod.regress(
        config["dir_name"],
        config["asset_id_col"],
        config["regress_config"]["target_col"],
        config["regress_config"]["feature_cols"],
        config["regress_config"]["feature_lag"],
        config["regress_config"]["batch_size"],
    )
    coefficients.head(3)
    corr.head()
