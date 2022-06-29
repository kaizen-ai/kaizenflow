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
# Compute and analyze model performance stats.

# %% [markdown]
# # Imports

# %%
# %load_ext autoreload
# %autoreload 2
# %matplotlib inline

# %%
import datetime
import logging
from typing import Any, Callable, List

import ipywidgets as widgets
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import sklearn

import core.config.config_ as cconconf
import core.config.config_utils as ccocouti
import core.statistics.sharpe_ratio as cstshrat
import dataflow.model as dtfmod
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hpandas as hpandas
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
def get_notebook_config() -> cconconf.Config:
    """
    Get notebook specific config.
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
        "stats_kwargs": {
            "quantile_ranks": 10,
            # 28500 is the number of 5-minute intervals in ATH in a year.
            "time_scaling": 28500,
            "n_resamples": 1000,
        },
        "plot_kwargs": {
            "y_min_lim_hit_rate": 0.45,
            "y_max_lim_hit_rate": 0.55,
            "color": "C0",
            "capsize": 0.2,
            "xticks_rotation": 70,
        },
    }
    config = ccocouti.get_config_from_nested_dict(param_dict)
    return config


# %%
config = get_notebook_config()
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


# TODO(Max): Move the code out of the lib so we can unit test,
# e.g., we want to add (small) specific unit tests for hit.
# TODO(Max): Harmonize the code with calculate_hit_rate and other code there.
# E.g., factor out the piece of calculate_hit_rate that computes hit, etc.
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
    # Drop NaNs to compute the performance statistics.
    metrics_df = hpandas.dropna(metrics_df, report_stats=True)
    # Compute hit and trade PnL.
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


# TODO(*): Consider using bootstraping function from SciPy
#  https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.bootstrap.html.
# TODO(Dan): Consider to return CI values.
def bootstrap(
    data: pd.Series, func: Callable, n_resamples: int = 100
) -> List[Any]:
    """
    Bootstrap computations on specified number of data resamples.

    :param data: input data to resample
    :param func: function accepting a series and returning a single scalar value
    :param n_resamples: number of resamples to create
    :return: bootstrapped computations
    """
    res_list = []
    for i in range(n_resamples):
        resampled_data = sklearn.utils.resample(data)
        res = func(resampled_data)
        res_list.append(res)
    return res_list


def plot_sharpe_ratio(
    config: cconconf.Config, metrics_df: pd.DataFrame, by_col: str
) -> None:
    """
    Compute and plot Sharpe Ratio by specified column.
    """
    res_list = []
    for by, data in metrics_df.groupby(by_col):
        srs = data[config["column_names"]["trade_pnl"]].dropna()
        func = lambda pnl: cstshrat.compute_sharpe_ratio(
            pnl, time_scaling=config["stats_kwargs"]["time_scaling"]
        )
        # Multiple Sharpe Ratios are being computed on many resamples
        # in order to find and plot confidence intervals.
        sharpe_ratio_srs = pd.Series(
            bootstrap(srs, func, config["stats_kwargs"]["n_resamples"]),
            name="sharpe_ratio",
        )
        # Transform and combine data for plotting.
        sharpe_ratio_df = sharpe_ratio_srs.to_frame()
        sharpe_ratio_df[by_col] = by
        res_list.append(sharpe_ratio_df)
    res_df = pd.concat(res_list)
    #
    sns.barplot(
        x=by_col,
        y="sharpe_ratio",
        data=res_df,
        color=config["plot_kwargs"]["color"],
        capsize=config["plot_kwargs"]["capsize"],
    )
    plt.xticks(rotation=config["plot_kwargs"]["xticks_rotation"])
    plt.show()


def plot_sorted_barplot(
    df: pd.DataFrame,
    x: str,
    y: str,
    sort_by: [str, bool],
    ascending: bool,
    ylabel: str,
    ylim_min: int,
    ylim_max: int,
) -> None:
    """
    :param df: data with prediction statistics
    :param x: column name for values on X-axis (e.g., `asset_id`)
    :param y: column name for values on Y-axis (e.g., `hit`)
    :param sort_by: sorting parameter (e.g., by value, by asset, or None)
    :param ylabel: name of the Y-axis graph
    :param ylim_min: lower value on Y-axis graph scale
    :param ylim_max: upper value on Y-axis graph scale
    :return: barplot with model performance statistics
    """
    if sort_by:
        stats_srs = df.groupby([x])[y].mean()
        stats_srs_sorted = stats_srs.reset_index().sort_values(
            by=[sort_by], ascending=ascending
        )
        order = stats_srs_sorted[x]
    else:
        order = None
    sns.barplot(
        x=x,
        y=y,
        data=df,
        order=order,
        color=color,
        capsize=capsize,
    )
    plt.xticks(rotation=xticks_rotation)
    plt.ylabel(ylabel)
    plt.ylim(ylim_min, ylim_max)
    plt.show()


def widget_plot(
    df: pd.DataFrame, x: str, y: str, ylim_min: int, ylim_max: int
) -> None:
    """
    Add widges to expand the sorting parameters for barplots.

    :param df: data with prediction statistics
    :param x: values on X-axis (e.g., `asset_id`)
    :param y: values on Y-axis (e.g., `hit`)
    :param ylim_min: lower value on Y-axis graph scale
    :param ylim_max: upper value on Y-axis graph scale
    :return: barplot with edible model performance statistics.
    """
    _ = widgets.interact(
        plot_sorted_barplot,
        df=widgets.fixed(df),
        x=widgets.fixed(x),
        y=widgets.fixed(y),
        sort_by=widgets.ToggleButtons(
            options=[y, x, False], description="Sort by:"
        ),
        ascending=widgets.ToggleButtons(
            options=[True, False], description="Ascending:"
        ),
        ylabel=widgets.fixed(f"{y}_rate"),
        ylim_min=widgets.FloatText(
            value=ylim_min,
            description="Min y-scale:",
        ),
        ylim_max=widgets.FloatText(
            value=ylim_max,
            description="Max y-scale:",
        ),
    )


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
y = config["column_names"]["y"]
y_hat = config["column_names"]["y_hat"]
hit = config["column_names"]["hit"]
trade_pnl = config["column_names"]["trade_pnl"]
#
quantile_ranks = config["stats_kwargs"]["quantile_ranks"]
y_min_lim_hit_rate = config["plot_kwargs"]["y_min_lim_hit_rate"]
y_max_lim_hit_rate = config["plot_kwargs"]["y_max_lim_hit_rate"]
color = config["plot_kwargs"]["color"]
capsize = config["plot_kwargs"]["capsize"]
xticks_rotation = config["plot_kwargs"]["xticks_rotation"]

# %% [markdown]
# ## By asset

# %% [markdown]
# ### Hit rate

# %%
widget_plot(
    metrics_df_reset_index, asset_id, hit, y_min_lim_hit_rate, y_max_lim_hit_rate
)

# %% [markdown]
# ### PnL

# %%
# Compute PnL for each asset id.
pnl_stats = (
    metrics_df.groupby(asset_id)[trade_pnl].sum().sort_values(ascending=False)
)
pnl_stats = pnl_stats.to_frame().reset_index()

# Plot PnL per asset id.
widget_plot(pnl_stats, "asset_id", trade_pnl, 0, 450)

# %%
# Plot cumulative PnL over time per asset id.
_ = metrics_df[trade_pnl].dropna().unstack().cumsum().plot()

# %% run_control={"marked": false}
# Plot average trade PnL per asset id.
widget_plot(metrics_df_reset_index, asset_id, trade_pnl, 0, 0.005)

# %% [markdown]
# ### Sharpe Ratio

# %%
plot_sharpe_ratio(config, metrics_df_reset_index, asset_id)

# %% [markdown]
# ## By time

# %%
metrics_df_reset_index["hour"] = metrics_df_reset_index[timestamp].dt.hour
metrics_df_reset_index["weekday"] = metrics_df_reset_index[
    timestamp
].dt.day_name()
metrics_df_reset_index["month"] = metrics_df_reset_index[
    timestamp
].dt.month_name()

# %% [markdown]
# ### Hit Rate

# %%
widget_plot(
    metrics_df_reset_index, "hour", hit, y_min_lim_hit_rate, y_max_lim_hit_rate
)

# %%
widget_plot(
    metrics_df_reset_index, "weekday", hit, y_min_lim_hit_rate, y_max_lim_hit_rate
)

# %%
widget_plot(
    metrics_df_reset_index, "month", hit, y_min_lim_hit_rate, y_max_lim_hit_rate
)

# %% [markdown]
# ### PnL

# %%
widget_plot(metrics_df_reset_index, "hour", trade_pnl, 0, 0.005)

# %%
widget_plot(metrics_df_reset_index, "weekday", trade_pnl, 0, 0.004)

# %%
widget_plot(metrics_df_reset_index, "month", trade_pnl, 0, 0.0055)

# %% [markdown]
# ### Sharpe Ratio

# %%
plot_sharpe_ratio(config, metrics_df_reset_index, "hour")

# %%
plot_sharpe_ratio(config, metrics_df_reset_index, "weekday")

# %%
plot_sharpe_ratio(config, metrics_df_reset_index, "month")

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
widget_plot(
    metrics_df_reset_index,
    prediction_magnitude,
    hit,
    y_min_lim_hit_rate,
    y_max_lim_hit_rate,
)

# %% [markdown]
# ### PnL

# %%
widget_plot(metrics_df_reset_index, prediction_magnitude, trade_pnl, 0, 0.01)

# %% [markdown]
# ### Sharpe Ratio

# %%
plot_sharpe_ratio(config, metrics_df_reset_index, prediction_magnitude)

# %% [markdown]
# ## By volume

# %%
volume_quantile = ".".join([volume, "quantile_rank"])
metrics_df_reset_index[volume_quantile] = metrics_df_reset_index.groupby(
    asset_id
)[volume].transform(lambda x: pd.qcut(x, quantile_ranks, labels=False))

# %% [markdown]
# ### Hit rate

# %%
widget_plot(
    metrics_df_reset_index,
    volume_quantile,
    hit,
    y_min_lim_hit_rate,
    y_max_lim_hit_rate,
)

# %% [markdown]
# ### PnL

# %%
widget_plot(metrics_df_reset_index, volume_quantile, trade_pnl, 0.0005, 0.0045)

# %% [markdown]
# ### Sharpe Ratio

# %%
plot_sharpe_ratio(config, metrics_df_reset_index, volume_quantile)
