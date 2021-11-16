# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.13.0
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Imports

# %%
import os

import pandas as pd
import seaborn as sns
from statsmodels.tsa.stattools import adfuller

import core.config.config_ as cconconf
import core.plotting as cplot
import helpers.s3 as hs3
import research.cc.statistics as rccstat

# %% [markdown]
# # Config

# %%
def get_cmtask323_config() -> cconconf.Config:
    """
    Get task323-specific config.
    """
    config = cconconf.Config()
    # Load parameters.
    config.add_subconfig("load")
    config["load"]["aws_profile"] = "am"
    config["load"]["data_dir"] = os.path.join(hs3.get_path(), "data")
    # Data parameters.
    config.add_subconfig("data")
    config["data"]["data_type"] = "OHLCV"
    config["data"]["universe_version"] = "v0_3"
    #        config["data"]["universe_version"] = "v0_1"
    config["data"]["vendor"] = "CCXT"
    # Column names.
    config.add_subconfig("column_names")
    config["column_names"]["volume"] = "volume"
    config["column_names"]["currency_pair"] = "currency_pair"
    config["column_names"]["exchange"] = "exchange_id"
    config["column_names"]["close"] = "close"
    return config


config = get_cmtask323_config()
print(config)


# %% [markdown]
# # Functions

# %%
def compute_volatility_for_each_coin(data, freq):
    """
    Loads and transforms each (exchange-coin) dataframe to compute 18-period
    ema volatility.

    Parameters: DataFrame, resampling frequency
    """
    data["date"] = data.index
    new_df = data.groupby(
        ["currency_pair", "exchange_id", pd.Grouper(key="date", freq=freq)]
    )["close"].last()
    new_df = new_df.pct_change().transform(
        lambda x: x.ewm(span=18, adjust=False).std()
    )
    new_df = new_df.reset_index()
    return new_df


def daily_close(data, freq):
    """
    Loads and transforms each (exchange-coin) dataframe to compute volatility
    for the whole period.

    Parameters: DataFrame, resampling frequency
    """
    data["date"] = data.index
    new_df = data.groupby(
        ["currency_pair", "exchange_id", pd.Grouper(key="date", freq=freq)]
    )["close"].last()
    new_df = new_df.reset_index()
    return new_df


def get_df_with_coin_price_volatility(data, display_plot):
    """
    Unifies volatility values for each coin and plot the graph.

    Parameters: DataFrame with computed volatility, boolean value to plot the graph
    """
    vix_df = data.groupby(
        ["currency_pair", pd.Grouper(key="date", freq=frequency)]
    )["close"].mean()
    vix_df = vix_df.to_frame()
    vix_df.columns = ["ema_volatility"]
    if display_plot:
        sns.set(rc={"figure.figsize": (15, 8)})
        sns.lineplot(
            data=vix_df, x="date", y="ema_volatility", hue="currency_pair"
        )
    return vix_df


def get_overall_returns_volatility(data, display_plot):
    """
    Unifies volatility values for each coin for the whole period and plot the
    barplot.

    Parameters: DataFrame with computed volatility, boolean value to plot the graph
    """
    close_df = daily_close.groupby(
        ["currency_pair", pd.Grouper(key="date", freq=frequency)]
    )["close"].mean()
    rets_df = close_df.groupby(["currency_pair"]).pct_change()
    std_df = rets_df.groupby(["currency_pair"]).std()
    if display_plot:
        cplot.plot_barplot(
            std_df.sort_values(ascending=False),
            title="Volatility per coin for the whole period (1-day basis, log-scaled)",
            figsize=[15, 7],
            yscale="log",
        )
    return std_df


def perform_adf_test(df_daily):
    """
    Performs ADF test to check the stationarity of volatility values
    Parameters: Daily DataFrame with computed volatility
    """
    final_result = []
    coin_list = df_daily.reset_index()["currency_pair"].unique()
    for coin in coin_list:
        result = pd.DataFrame()
        df = df_daily.loc[[coin]]
        df = df[df["ema_volatility"].notna()].copy()
        X = df["ema_volatility"].values
        test_result = adfuller(X)
        result.loc[f"{coin}", "ADF Statistic"] = test_result[0]
        result.loc[f"{coin}", "p-value"] = test_result[1]
        final_result.append(result)
    final_result = pd.concat(final_result)
    final_result["is_unit_root_and_non-stationary (5% sign. level)"] = (
        final_result["p-value"] > 0.05
    )
    return final_result


# %% [markdown]
# # Volatility Analysis

# %% [markdown]
# ## 1 day

# %% run_control={"marked": false}
frequency = "1D"
compute_daily_vix_ema = lambda data: compute_volatility_for_each_coin(
    data, freq=frequency
)
daily_vix_ema = rccstat.compute_stats_for_universe(config, compute_daily_vix_ema)

# %%
ema_df_daily = get_df_with_coin_price_volatility(daily_vix_ema, display_plot=True)
display(ema_df_daily)

# %% [markdown]
# ## 5 min

# %%
frequency = "5min"
compute_5min_vix_ema = lambda data: compute_volatility_for_each_coin(
    data, freq=frequency
)
vix_ema_5min = rccstat.compute_stats_for_universe(config, compute_5min_vix_ema)

# %%
ema_df_5min = get_df_with_coin_price_volatility(vix_ema_5min, display_plot=True)
display(ema_df_5min)

# %% [markdown]
# ## Volatility for the whole period (1-day frequency)

# %%
frequency = "1D"
compute_daily_close = lambda data: daily_close(data, freq=frequency)
daily_close = rccstat.compute_stats_for_universe(config, compute_daily_close)

# %%
std_df = get_overall_returns_volatility(daily_close, display_plot=True)
display(std_df)

# %% [markdown]
# # Test for stationarity of volatility

# %%
test_results = perform_adf_test(ema_df_daily)
display(test_results)

# %% [markdown]
# After test results we see that __FIL/USDT__ volatility over 1-day is failed to pass the stationarity test. The graph below confirms the persistence of trend: seems like the coin was too volatile right after the listing and failed to keep the same levels during its trading lifetime.

# %%
sns.lineplot(
    data=ema_df_daily.loc[["FIL/USDT"]].reset_index(),
    x="date",
    y="ema_volatility",
    hue="currency_pair",
)
