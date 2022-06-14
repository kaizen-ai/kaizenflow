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

# %% [markdown]
# # Description

# %% [markdown]
# Compute stats from ML pipeline result.

# %% [markdown]
# # Imports

# %%
# %load_ext autoreload
# %autoreload 2
# %matplotlib inline

# %%
import datetime
import logging
from typing import Optional

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

import core.config.config_ as cconconf
import core.config.config_utils as ccocouti
import dataflow.model as dtfmod
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hprint as hprint
import im_v2.crypto_chassis.data.client as iccdc

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()


# %% [markdown]
# # Configs

# %%
def get_master_ml_config() -> cconconf.Config:
    """
    Get Master ML pipeline specific config.
    """
    config = cconconf.Config()
    param_dict = {
        "data": {
            "dir_name": "/shared_data/model/historical/experiment.E1a.crypto_chassis_v2-all.5T.2018_2022/tiled_results/",
            "columns": "volume vwap vwap.ret_0 vwap.ret_0.vol_adj vwap.ret_0.vol_adj.c vwap.ret_0.vol_adj_2 vwap.ret_0.vol_adj_2_hat".split(),
            "start_date": datetime.date(2018, 1, 1),
            "end_date": datetime.date(2022, 5, 1),
        },
        "column_names": {
            "asset_id": "asset_id",
            "timestamp": "end_ts",
            "volume": "volume",
            "y": "vwap.ret_0.vol_adj_2",
            "y_hat": "vwap.ret_0.vol_adj_2_hat",
            "hit_rate": "hit",
            "pnl": "trade_pnl",
        },
    }
    config = ccocouti.get_config_from_nested_dict(param_dict)
    return config


# %%
config = get_master_ml_config()
print(config)


# %% [markdown]
# # Functions

# %%
def load_predictions_df(config: cconconf.Config) -> pd.DataFrame:
    """
    Get data with ML predictions.
    """
    backtest_df_iter = dtfmod.yield_processed_parquet_tiles_by_year(
    config["data"]["dir_name"],
    config["data"]["start_date"],
    config["data"]["end_date"],
    config["column_names"]["asset_id"],
    data_cols=config["data"]["columns"],
    asset_ids=None,
    )
    #
    dfs = []
    for df in backtest_df_iter:
        dfs.append(df)
    #
    predict_df = pd.concat(dfs)
    predict_df = predict_df.sort_index()
    return predict_df


def preprocess_predictions_df(config: cconconf.Config, predict_df: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocess data with ML predictions for analysis.
    """
    # Convert the prediction stats data to Multiindex by time and asset id.
    metrics_df = predict_df.stack()
    # Compute hit and PnL.
    metrics_df["hit"] = metrics_df[config["column_names"]["y"]] * metrics_df[config["column_names"]["y_hat"]] >= 0
    metrics_df["trade_pnl"] = metrics_df[config["column_names"]["y"]] * metrics_df[config["column_names"]["y_hat"]]
    # TODO(*): Think about avoiding using `ImClient` for mapping.
    # Convert asset ids to full symbols using `ImClient` mapping.
    resample_1min = True
    im_client = iccdc.get_CryptoChassisHistoricalPqByTileClient_example2(
        resample_1min
    )
    metrics_df.index = metrics_df.index.set_levels(
        metrics_df.index.levels[1].map(im_client._asset_id_to_full_symbol_mapping),
        level=1,
    )
    return metrics_df


def plot_cum_pnl_per_asset_id(
    config: cconconf.Config,
    metrics_df: pd.DataFrame,
    *,
    color: str = "C0",
    capsize: float = 0.2,
    xticks_rotation: int = 70,
) -> pd.DataFrame:
    """
    Plot cummulative PnL per asset id overall and by time.
    """
    # Cummulative PnL for a given coin.
    pnl_stats = metrics_df.groupby(
        config["column_names"]["asset_id"]
    )[config["column_names"]["pnl"]].sum().sort_values(ascending=False)
    # Plot overall cummulative PnL per asset id.
    _ = sns.barplot(
        x=pnl_stats.index, y=pnl_stats.values, color=color, capsize=capsize
    )
    plt.xticks(rotation=xticks_rotation)
    plt.show()
    # Plot cummulative PnL over time per asset id.
    _ = metrics_df[config["column_names"]["pnl"]].dropna().unstack().cumsum().plot()
    plt.show()

    
def plot_metric(
    config: cconconf.Config,
    metrics_df: pd.DataFrame,
    metric: str,
    by: str,
    *,
    y_min_lim: Optional[float] = None,
    y_max_lim: Optional[float] = None,
    decile_ranks: int = 10,
    color: str = "C0",
    capsize: float = 0.2,
    xticks_rotation: int = 70,
) -> None:
    """
    Plot requested metrics by specified column.
    """
    data = metrics_df.reset_index().copy()
    # Set X-axis column name and compute data for it if needed.
    if by == "asset_id":
        x_name = config["column_names"]["asset_id"]
    elif by in ["hour", "weekday", "month"]:
        x_name = by
        if by == "hour":
            data[x_name] = data[config["column_names"]["timestamp"]].dt.hour
        elif by == "weekday":
            data[x_name] = data[config["column_names"]["timestamp"]].dt.day_name()
        else:
            data["month"] = data[config["column_names"]["timestamp"]].dt.month_name()
    elif by == "prediction_magnitude":
        x_name = ".".join([config["column_names"]["y_hat"], "decile_rank"])
        # Make a columns with prediction decile ranks.
        data[x_name] = pd.qcut(data[config["column_names"]["y_hat"]], decile_ranks, labels = False)
    elif by == "volume":
        x_name = ".".join([config["column_names"]["volume"], "decile_rank"])
        # Make a columns with volume decile ranks per asset id.
        data[x_name] = data.groupby(
            config["column_names"]["asset_id"]
        )[config["column_names"]["volume"]].transform(
            lambda x: pd.qcut(x, decile_ranks, labels=False)
        )
    else:
        raise
    # Set Y-axis column name and comute data for it if needed.
    if metric == "pnl":
        y_name = config["column_names"]["pnl"]
    elif metric == "hit_rate":
        y_name = config["column_names"]["hit_rate"]
        # Set limits around default values interval.
        y_min_lim = y_min_lim or 0.45
        y_max_lim = y_max_lim or 0.55
    elif metric == "sharpe_ratio":
        y_name = metric
        # Compute Shapre Ratio per X-axis category.
        data = data.groupby(x_name)[
            config["column_names"]["pnl"]
        ].agg(
            lambda x: x.mean() / x.std()
        ).sort_values(ascending=False).reset_index()
        # Rename columns to correctly access PnL data.
        data = data.rename(columns={config["column_names"]["pnl"]: y_name})
    else:
        raise
    #
    sns.barplot(
        x=x_name, y=y_name, data=data, color=color, capsize=capsize
    )
    plt.xticks(rotation=xticks_rotation)
    plt.ylim(y_min_lim, y_max_lim)
    plt.show()


# %% [markdown]
# # Load data with predictions

# %%
predict_df = load_predictions_df(config)
print(predict_df.shape)
predict_df.head(3)

# %% [markdown]
# # Compute overall PnL

# %%
(
    predict_df[config["column_names"]["y"]] * 
    predict_df[config["column_names"]["y_hat"]]
).sum(axis=1).cumsum().plot()

# %% [markdown]
# # Get data for analysis

# %%
metrics_df = preprocess_predictions_df(config, predict_df)
metrics_df.head()

# %% [markdown]
# # Stats

# %% [markdown]
# ## By asset

# %% [markdown]
# ### Hit rate

# %%
_ = plot_metric(config, metrics_df, "hit_rate", "asset_id")

# %% [markdown]
# ### PnL

# %%
_ = plot_cum_pnl_per_asset_id(config, metrics_df)

# %%
_ = plot_metric(config, metrics_df, "pnl", "asset_id")

# %% [markdown]
# ### Sharpe Ratio

# %%
_ = plot_metric(config, metrics_df, "sharpe_ratio", "asset_id")

# %% [markdown]
# ## By time

# %% [markdown]
# ### Hit Rate

# %%
_ = plot_metric(config, metrics_df, "hit_rate", "hour", y_min_lim=0.4)

# %%
_ = plot_metric(config, metrics_df, "hit_rate", "weekday")

# %%
_ = plot_metric(config, metrics_df, "hit_rate", "month")

# %% [markdown]
# ### PnL

# %%
_ = plot_metric(config, metrics_df, "pnl", "hour")

# %%
_ = plot_metric(config, metrics_df, "pnl", "weekday")

# %%
_ = plot_metric(config, metrics_df, "pnl", "month")

# %% [markdown]
# ## By prediction magnitude

# %% [markdown]
# ### Hit rate

# %%
_ = plot_metric(config, metrics_df, "hit_rate", "prediction_magnitude")

# %% [markdown]
# ### PnL

# %%
_ = plot_metric(config, metrics_df, "pnl", "prediction_magnitude")

# %% [markdown]
# ## By volume

# %% [markdown]
# ### Hit rate

# %%
_ = plot_metric(config, metrics_df, "hit_rate", "volume")

# %% [markdown]
# ### PnL

# %%
_ = plot_metric(config, metrics_df, "pnl", "volume")

# %%
