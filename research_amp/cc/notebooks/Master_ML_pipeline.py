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
from typing import Optional, Tuple

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
            "im_client": iccdc.get_CryptoChassisHistoricalPqByTileClient_example2(
                True
            ),
        },
        "column_names": {
            "asset_id": "asset_id",
            "timestamp": "end_ts",
            "volume": "volume",
            "y": "vwap.ret_0.vol_adj_2",
            "y_hat": "vwap.ret_0.vol_adj_2_hat",
            "hit": "hit",
            "trade_pnl": "trade_pnl",
        },
        "plot_kwargs": {
            "y_min_lim": 0.4,
            "y_max_lim": 0.6,
            "quantile_ranks": 10,
            "color": "C0",
            "capsize": 0.2,
            "xticks_rotation": 70,
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


def preprocess_predictions_df(
    config: cconconf.Config, predict_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Preprocess data with ML predictions for analysis.

    Input:

    ```
                        volume                  vwap
    asset_id            1464553467  1467591036  1464553467  1467591036
    end_ts
    2018-01-01 09:35:00   314.0657     47.3976    729.7789  12887.3945
    2018-01-01 09:40:00   178.6543     35.1098    731.0134  12913.6854
    ```

    Output:

    ```
                                              volume        vwap
    end_ts                        asset_id
    2018-01-01 09:35:00  binance::ETH_USDT  314.0657    729.7789
                         binance::BTC_USDT   47.3976  12887.3945
    2018-01-01 09:40:00  binance::ETH_USDT  178.6543    731.0134
                         binance::BTC_USDT   35.1098  12913.6854
    ```
    """
    # Convert the prediction stats data to Multiindex by time and asset id.
    metrics_df = predict_df.stack()
    # Compute hit and PnL.
    metrics_df["hit"] = (
        metrics_df[config["column_names"]["y"]]
        * metrics_df[config["column_names"]["y_hat"]]
        >= 0
    )
    metrics_df["trade_pnl"] = (
        metrics_df[config["column_names"]["y"]]
        * metrics_df[config["column_names"]["y_hat"]]
    )
    # TODO(*): Think about avoiding using `ImClient` for mapping.
    # Convert asset ids to full symbols using `ImClient` mapping.
    im_client = config["data"]["im_client"]
    metrics_df.index = metrics_df.index.set_levels(
        metrics_df.index.levels[1].map(
            im_client._asset_id_to_full_symbol_mapping
        ),
        level=1,
    )
    return metrics_df


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
    predict_df[config["column_names"]["y"]]
    * predict_df[config["column_names"]["y_hat"]]
).sum(axis=1).cumsum().plot()

# %% [markdown]
# # Get data for analysis

# %%
metrics_df = preprocess_predictions_df(config, predict_df)
metrics_df.head()

# %%
# Reset index to ease further preprocessing.
# TODO(Dan): Move index resetting under plotting funtions.
metrics_df_reset_index = metrics_df.reset_index()

# %% [markdown]
# # Stats

# %%
# Set oftenly used config parameters.
asset_id = config["column_names"]["asset_id"]
timestamp = config["column_names"]["timestamp"]
volume = config["column_names"]["volume"]
y_hat = config["column_names"]["y_hat"]
hit = config["column_names"]["hit"]
trade_pnl = config["column_names"]["trade_pnl"]
#
y_min_lim = config["plot_kwargs"]["y_min_lim"]
y_max_lim = config["plot_kwargs"]["y_max_lim"]
quantile_ranks = config["plot_kwargs"]["quantile_ranks"]
color = config["plot_kwargs"]["color"]
capsize = config["plot_kwargs"]["capsize"]
xticks_rotation = config["plot_kwargs"]["xticks_rotation"]

# %% [markdown]
# ## By asset

# %% [markdown]
# ### Hit rate

# %%
sns.barplot(
    x=asset_id,
    y=hit,
    data=metrics_df_reset_index,
    color=color,
    capsize=capsize,
)
#
plt.xticks(rotation=xticks_rotation)
plt.ylabel("hit_rate")
plt.ylim(y_min_lim, y_max_lim)
plt.show()

# %% [markdown]
# ### PnL

# %%
# Summary PnL for a given coin.
pnl_stats = metrics_df.groupby(asset_id)[trade_pnl].sum().sort_values(ascending=False)
# Plot summary PnL per asset id.
_ = sns.barplot(
    x=pnl_stats.index,
    y=pnl_stats.values,
    color=color,
    capsize=capsize,
)
plt.xticks(rotation=xticks_rotation)
plt.show()

# %%
# Plot cumulative PnL over time per asset id.
_ = metrics_df[trade_pnl].dropna().unstack().cumsum().plot()

# %%
# Plot average PnL per asset id.
sns.barplot(
    x=asset_id,
    y=trade_pnl,
    data=metrics_df_reset_index,
    color=color,
    capsize=capsize,
)
#
plt.xticks(rotation=xticks_rotation)
plt.ylabel("avg_pnl")
plt.show()

# %% [markdown]
# ### Sharpe Ratio

# %%
# Compute Shapre Ratio asset id.
sr_data = (
    metrics_df.groupby(asset_id)[trade_pnl]
    .agg(lambda x: x.mean() / x.std())
    .sort_values(ascending=False)
    .reset_index()
)
# Plot Sharpe Ratio per asset id.
sns.barplot(
    x=asset_id,
    y=trade_pnl,
    data=sr_data,
    color=color,
    capsize=capsize,
)
#
plt.xticks(rotation=xticks_rotation)
plt.ylabel("sharpe_ratio")
plt.show()

# %% [markdown]
# ## By time

# %%
metrics_df_reset_index["hour"] = metrics_df_reset_index[timestamp].dt.hour
metrics_df_reset_index["weekday"] = metrics_df_reset_index[timestamp].dt.day_name()
metrics_df_reset_index["month"] = metrics_df_reset_index[timestamp].dt.month_name()

# %% [markdown]
# ### Hit Rate

# %%
sns.barplot(
    x="hour",
    y=hit,
    data=metrics_df_reset_index,
    color=color,
    capsize=capsize,
)
#
plt.xticks(rotation=xticks_rotation)
plt.ylabel("hit_rate")
plt.ylim(y_min_lim, y_max_lim)
plt.show()

# %%
sns.barplot(
    x="weekday",
    y=hit,
    data=metrics_df_reset_index,
    color=color,
    capsize=capsize,
)
#
plt.xticks(rotation=xticks_rotation)
plt.ylabel("hit_rate")
plt.ylim(y_min_lim, y_max_lim)
plt.show()

# %%
sns.barplot(
    x="month",
    y=hit,
    data=metrics_df_reset_index,
    color=color,
    capsize=capsize,
)
#
plt.xticks(rotation=xticks_rotation)
plt.ylabel("hit_rate")
plt.ylim(y_min_lim, y_max_lim)
plt.show()

# %% [markdown]
# ### PnL

# %%
sns.barplot(
    x="hour",
    y=trade_pnl,
    data=metrics_df_reset_index,
    color=color,
    capsize=capsize,
)
#
plt.xticks(rotation=xticks_rotation)
plt.ylabel("avg_pnl")
plt.show()

# %%
sns.barplot(
    x="weekday",
    y=trade_pnl,
    data=metrics_df_reset_index,
    color=color,
    capsize=capsize,
)
#
plt.xticks(rotation=xticks_rotation)
plt.ylabel("avg_pnl")
plt.show()

# %%
sns.barplot(
    x="month",
    y=trade_pnl,
    data=metrics_df_reset_index,
    color=color,
    capsize=capsize,
)
#
plt.xticks(rotation=xticks_rotation)
plt.ylabel("avg_pnl")
plt.show()

# %% [markdown]
# ## By prediction magnitude

# %%
prediction_magnitude = ".".join([y_hat, "quantile_rank"])
metrics_df_reset_index[prediction_magnitude] = pd.qcut(
    metrics_df_reset_index[y_hat], quantile_ranks, labels=False
)

# %% [markdown]
# ### Hit rate

# %%
sns.barplot(
    x=prediction_magnitude,
    y=hit,
    data=metrics_df_reset_index,
    color=color,
    capsize=capsize,
)
#
plt.xticks(rotation=xticks_rotation)
plt.ylabel("hit_rate")
plt.ylim(y_min_lim, y_max_lim)
plt.show()

# %% [markdown]
# ### PnL

# %%
sns.barplot(
    x=prediction_magnitude,
    y=trade_pnl,
    data=metrics_df_reset_index,
    color=color,
    capsize=capsize,
)
#
plt.xticks(rotation=xticks_rotation)
plt.ylabel("avg_pnl")
plt.show()

# %% [markdown]
# ## By volume

# %%
volume_quantile = ".".join([volume, "quantile_rank"])
metrics_df_reset_index[volume_quantile] = metrics_df_reset_index.groupby(asset_id)[volume].transform(
    lambda x: pd.qcut(x, quantile_ranks, labels=False)
)

# %% [markdown]
# ### Hit rate

# %%
sns.barplot(
    x=volume_quantile,
    y=hit,
    data=metrics_df_reset_index,
    color=color,
    capsize=capsize,
)
#
plt.xticks(rotation=xticks_rotation)
plt.ylabel("hit_rate")
plt.ylim(y_min_lim, y_max_lim)
plt.show()

# %% [markdown]
# ### PnL

# %%
sns.barplot(
    x=volume_quantile,
    y=trade_pnl,
    data=metrics_df_reset_index,
    color=color,
    capsize=capsize,
)
#
plt.xticks(rotation=xticks_rotation)
plt.ylabel("avg_pnl")
plt.show()

# %%
