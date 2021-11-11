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
import logging
import os

import pandas as pd
import seaborn as sns
from statsmodels.tsa.stattools import adfuller

import core.config.config_ as ccocon
import helpers.dbg as hdbg
import helpers.env as henv
import helpers.printing as hprintin
import helpers.s3 as hs3
import im.data.universe as imdauni
import research.cc.statistics as rccsta
import research.cc.volume as rccvol
import im.ccxt.data.load.loader as imccdaloloa

import core.plotting as cplot
import numpy as np


# %% [markdown]
# # Config

# %%
def get_cmtask323_config() -> ccocon.Config:
    """
    Get task323-specific config.
    """
    config = ccocon.Config()
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
def get_initial_df_with_close_prices(coins, exchange, is_notional_volume):
    """
    Return DataFrame with the volume of all coins for exchange with initial timestamps
    Parameters: list of coins, exchange name
    """
    result = []
    loader = imccdaloloa.CcxtLoader(root_dir="s3://alphamatic-data/data", aws_profile="am")
    for coin in coins:
        df = loader.read_data_from_filesystem(
            exchange_id=exchange, currency_pair=coin, data_type="OHLCV"
        )
        df = df[["close", "currency_pair", "exchange_id"]]
        df["date"] = df.index
        df = df.reset_index(drop=True)
        result.append(df)
    final_result = pd.concat(result)
    return final_result

def get_df_with_price_volatility(df, freq):
    resample = df.groupby(["currency_pair", pd.Grouper(key="date", freq=freq)])["close"].last()
    resample = resample.to_frame()
    resample["rets"] = resample.groupby(["currency_pair"]).close.pct_change()
    resample['std_18_ema_simple'] = resample.groupby(['currency_pair'])['rets'].transform(lambda x: x.ewm(span=18, adjust=False).std())
    rets_for_plot = resample.reset_index()
    sns.set(rc={'figure.figsize':(15,8)})
    sns.lineplot(data=rets_for_plot, x='date', y='std_18_ema_simple', hue="currency_pair")
    return resample

def perform_adf_test(df_daily, coin_list):
    final_result = []
    for coin in coin_list:
            result = pd.DataFrame()
            df = df_daily.loc[[f"{coin}"]]
            df[df["std_18_ema_simple"].notna()]
            X = df[df["std_18_ema_simple"].notna()]["std_18_ema_simple"].values
            test_result = adfuller(X)
            result.loc[f"{coin}", "ADF Statistic"] = test_result[0]
            result.loc[f"{coin}", "p-value"] = test_result[1]
#            result.loc[f"{coin}", "is_unit_root_and_non-stationary (5% sign. level)"] = (result["p-value"]>0.05)
            final_result.append(result)
    final_result = pd.concat(final_result)
    final_result["is_unit_root_and_non-stationary (5% sign. level)"] = (final_result["p-value"]>0.05)
    return final_result


# %% [markdown]
# # Load the data

# %%
# get the list of all coin paires for each exchange
binance_coins = imdauni.get_trade_universe("v0_3")["CCXT"]["binance"]
ftx_coins = imdauni.get_trade_universe("v0_3")["CCXT"]["ftx"]
gateio_coins = imdauni.get_trade_universe("v0_3")["CCXT"]["gateio"]
kucoin_coins = imdauni.get_trade_universe("v0_3")["CCXT"]["kucoin"]

# load all the dataframes
binance = get_initial_df_with_close_prices(binance_coins, "binance", is_notional_volume=True)
ftx = get_initial_df_with_close_prices(ftx_coins, "ftx", is_notional_volume=True)
gateio = get_initial_df_with_close_prices(gateio_coins, "gateio", is_notional_volume=True)
kucoin = get_initial_df_with_close_prices(kucoin_coins, "kucoin", is_notional_volume=True)

# construct unique DataFrame
prices_df = pd.concat([binance, ftx, gateio, kucoin])
prices_df.head(3)

# %% [markdown]
# # Volatility Analysis

# %% [markdown]
# ## 1 day

# %%
ema_df_daily = get_df_with_price_volatility(prices_df, "1D")
ema_df_daily

# %%
cplot.plot_barplot(
    ema_df_daily.groupby(["currency_pair"])["rets"].std().sort_values(ascending=False),
    title="Volatility per coin for the whole period (1-day basis, log-scaled)",
    figsize=[15, 7],
    yscale="log",
)

# %% [markdown]
# ## 5 min

# %%
ema_df_5min = get_df_with_price_volatility(prices_df, "5min")
ema_df_5min

# %%
cplot.plot_barplot(
    ema_df_5min.groupby(["currency_pair"])["rets"].std().sort_values(ascending=False),
    title="Volatility per coin for the whole period (5min basis, log-scaled)",
    figsize=[15, 7],
    yscale="log",
)

# %% [markdown]
# One can notice that volatility distribution across coins is pretty stable if comparing between 1-day and 5-min sampling periods.

# %% [markdown]
# ## 1min

# %%
#ema_df_1min = get_df_with_price_volatility(prices_df, "1min")
#ema_df_1min

# %%
#cplot.plot_barplot(
#    ema_df_1min.groupby(["currency_pair"])["rets"].std().sort_values(ascending=False),
#    title="Volatility per coin for the whole period (1min basis, log-scaled)",
#    figsize=[15, 7],
#    yscale="log",
#)

# %% [markdown]
# # Test for stationarity of volatility

# %%
coin_list = ema_df_daily.reset_index()["currency_pair"].unique()
test_results = perform_adf_test(ema_df_daily, coin_list)
test_results

# %% [markdown]
# After test results we see that __FIL/USDT__ volatility over 1-day is failed to pass the stationarity test. The graph below confirms the persistence of trend: seems like the coin was too volatile right after the listing and failed to keep the same levels during its trading lifetime.

# %%
sns.lineplot(data=ema_df_daily.loc[["FIL/USDT"]].reset_index(), x='date', y='std_18_ema_simple', hue="currency_pair")
