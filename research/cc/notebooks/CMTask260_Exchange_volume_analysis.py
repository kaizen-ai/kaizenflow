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

import core.config.config_ as ccocon
import helpers.dbg as hdbg
import helpers.env as henv
import helpers.printing as hprintin
import helpers.s3 as hs3
import im.data.universe as imdauni
import research.cc.statistics as rccsta
import research.cc.volume as rccvol

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprintin.config_notebook()


# %% [markdown]
# # Config

# %%
def get_cmtask260_config() -> ccocon.Config:
    """
    Get task260-specific config.
    """
    config = ccocon.Config()
    # Load parameters.
    config.add_subconfig("load")
    config["load"]["aws_profile"] = "am"
    config["load"]["data_dir"] = os.path.join(hs3.get_path(), "data")
    # Data parameters.
    config.add_subconfig("data")
    config["data"]["data_type"] = "OHLCV"
    config["data"]["universe_version"] = "v0_2"
    # Column names.
    config.add_subconfig("column_names")
    config["column_names"]["volume"] = "volume"
    config["column_names"]["currency_pair"] = "currency_pair"
    config["column_names"]["exchange"] = "exchange_id"
    config["column_names"]["close"] = "close"
    return config


config = get_cmtask260_config()
print(config)

# %%
# loader = imccdaloloa.CcxtLoader(
#    root_dir="s3://alphamatic-data/data", aws_profile="am"
# )

# %% [markdown]
# # Load the data

# %%
compute_cum_volume = lambda data: rccvol.compute_cum_volume(
    data, config, nom_volume=False
)

cum_volume = rccsta.compute_stats_for_universe(config, compute_cum_volume)

# %%
_LOG.info("The number of exchanges - currency pairs =%s", cum_volume.shape[0])
cum_volume.head(3)

# %% [markdown]
# # Compute total volume per exchange

# %%
total_volume_by_exchange = rccvol.get_total_volume_by_exchange(
    cum_volume, config, avg_daily=False, display_plot=True
)
print(total_volume_by_exchange)

# %% [markdown]
# # Compute total volume per currency

# %%
total_volume_by_coins = rccvol.get_total_volume_by_coins(
    cum_volume, config, avg_daily=False, display_plot=True
)
print(total_volume_by_coins)


# %% [markdown]
# # Issue with compute_stats_for_universe()

# %% [markdown]
# As one can see, __compute_stats_for_universe()__ returns DataFrame with omitted timestamp values that are necessary to plot graph for rolling volume.
#
# What do you think we should do in this case?
# - We can either add param to your initial function that doesn't drop timestamp values
# - Or write the new one that takes into account timestamp values

# %% run_control={"marked": false}
def stats_func(df):
    return df


dd = rccsta.compute_stats_for_universe(config, stats_func)

# %%
dd.head()


# %% [markdown]
# # OLD CODE

# %% [markdown] heading_collapsed=true
# # Functions

# %% [markdown] hidden=true
# Note: by "volume" I mean the standard output that is nominated in the number of coins

# %% hidden=true
def get_volume_df_for_exch(coins, exchange):
    """
    Return the DataFrame with a volume of all available coins for a given exchange \
    with timestamp transformation to one day
    Parameters: list of coins for a particular exchange, exchange name
    """
    result = []
    for coin in coins:
        df = config.read_data_from_filesystem(
            exchange_id=exchange, currency_pair=coin, data_type="OHLCV"
        )
        # transform timestamp into one-day format
        volume_df = pd.DataFrame(df.groupby(by=df.index.date)["volume"].sum())
        volume_df.columns = [
            col_name + f"_{suffixes[coin]}_{exchange}"
            for col_name in volume_df.columns
        ]
        result.append(volume_df)
    final_result = pd.concat(result, axis=1)
    return final_result


def get_total_trading_volume_by_coins(coin_list, exch_list):
    """
    Return the DataFrame with total trading volume and normalised trading volume (by day) \
    of all coins from all available exchanges
    Parameters: list of coin names, volume dataframes
    """
    df = pd.concat(exch_list, axis=1)
    volume_df = pd.DataFrame()
    for coin in coin_list:
        coin_cols = [col for col in df.columns if coin in col]
        coin_df = df[coin_cols]
        coin_df["total_volume"] = coin_df.sum(axis=1)
        total_volume_ = coin_df["total_volume"].sum()
        norm_volume_ = coin_df["total_volume"].sum() / len(
            coin_df[coin_df["total_volume"] != 0]
        )
        volume_df.loc[
            "{}".format(f"{coin}"), ("total_trading_volume_in_coins")
        ] = total_volume_
        volume_df.loc[
            "{}".format(f"{coin}"), ("daily_avg_coin_volume")
        ] = norm_volume_
    return volume_df.sort_values(
        by="total_trading_volume_in_coins", ascending=False
    )


def get_total_trading_volume_by_exchange(df_list):
    """
    Return the DataFrame with total trading volume on exchanges
    Parameters: volume dataframes
    """
    exch_volume = pd.DataFrame()
    for df in df_list:
        total_volume_ = df.sum().sum()
        norm_volume_ = df.sum().sum() / df.shape[0]
        exch_volume.loc[
            "{}".format(f"{df.name}"), "total_trading_volume_in_coins"
        ] = total_volume_
        exch_volume.loc[
            "{}".format(f"{df.name}"), "daily_avg_coin_volume"
        ] = norm_volume_
    return exch_volume.sort_values(
        by="total_trading_volume_in_coins", ascending=False
    )


def plot_rolling_volume_by_coins(coin_list, exch_list):
    """
    Return the graph of 90-days rolling volumes for each coin on all exchanges
    Parameters: list of all coin names, volume dataframes
    """
    df = pd.concat(exch_list, axis=1)
    rolling_df = []
    for coin in coin_list:
        coin_df = df[[col for col in df.columns if coin in col]]
        coin_df["total_volume"] = coin_df.sum(axis=1)
        coin_df[f"rolling_90_volume_{coin}"] = (
            coin_df["total_volume"].rolling(90).mean()
        )
        rolling_df.append(coin_df[f"rolling_90_volume_{coin}"])
    rolling_df = pd.concat(rolling_df, axis=1)
    rolling_df.plot(figsize=(12, 7))


def plot_rolling_volume_by_exchange(exch_list, exch_names):
    """
    Return the graph of 90-days rolling volumes for each exchanges for all coins
    Parameters: volume dataframes, volume dataframes' names
    """
    df = pd.concat(exch_list, axis=1)
    rolling_df = []
    for exch in exch_names:
        exch_cols = [col for col in df.columns if exch in col]
        exch_df = df[exch_cols]
        exch_df["total_volume"] = exch_df.sum(axis=1)
        exch_df[f"rolling_90_volume_{exch}"] = (
            exch_df["total_volume"].rolling(90).mean()
        )
        rolling_df.append(exch_df[f"rolling_90_volume_{exch}"])
    rolling_df = pd.concat(rolling_df, axis=1)
    rolling_df.plot(figsize=(12, 7))


def compare_weekdays_volumes(exch_list):
    """
    Return statistics and graphs with working days vs.

    weekends analysis
    Parameters: volume dataframes
    """
    # clean the existing dataframes from previously calculated volumes
    df = pd.concat(exch_list, axis=1)
    df = df[[col for col in df.columns if "total_volume" not in col]]
    df = df[[col for col in df.columns if "rolling_volume" not in col]]
    # calculate new volumes that sum up all coins and exchanges
    df["total_volume"] = df.sum(axis=1)
    # create column with ids for weekdays
    df["weekday"] = df.index.map(lambda x: x.weekday())
    # plot total amount of volume for each day
    df.groupby("weekday").total_volume.sum().plot.bar(figsize=(12, 7))
    # plot working days vs. weekends
    weekends = df[(df["weekday"] == 5) | (df["weekday"] == 6)]
    sns.displot(weekends, x="total_volume")
    weekdays = df[(df["weekday"] != 5) & (df["weekday"] != 6)]
    sns.displot(weekdays, x="total_volume")
    # calculate descriptive statistics for working days vs. weekends
    print("Descriptive statistics:")
    weeknd_stat = weekends["total_volume"].describe()
    weekdys_stat = weekdays["total_volume"].describe()
    weeknd_stat = pd.DataFrame(weeknd_stat)
    weekdys_stat = pd.DataFrame(weekdys_stat)
    stats = pd.concat([weeknd_stat, weekdys_stat], axis=1)
    stats.columns = ["weekends", "working_days"]
    print(stats)
    print(
        "The graph labels in respective order: Total Volume by weekdays, Distribution of Volume over weekends, Distribution of Volume over working days"
    )


def get_initial_df_with_volumes(coins, exchange):
    """
    Return DataFrame with the volume of all coins for exchange with initial timestamps
    Parameters: list of coins, exchange name
    """
    result = []
    for coin in coins:
        df = config.read_data_from_filesystem(
            exchange_id=exchange, currency_pair=coin, data_type="OHLCV"
        )
        result.append(df["volume"])
    final_result = pd.concat(result, axis=1)
    return final_result


def plot_ath_volumes_comparison(df_list):
    """
    Return the graph with the comparison of average minute total trading volume
    in ATH vs.

    non-ATH
    Parameters: dataframe with volumes from a given exchange
    """
    plot_df = []
    for df in df_list:
        df_ath = df.iloc[df.index.indexer_between_time("09:30", "16:00")]
        df_not_ath = df.loc[~df.index.isin(df_ath.index)]
        ath_stat = pd.DataFrame()
        ath_stat.loc[f"{df.name}", f"minute_avg_total_volume_ath_{df.name}"] = (
            df_ath.sum().sum() / df_ath.shape[0]
        )
        ath_stat.loc[
            f"{df.name}", f"minute_avg_total_volume_not_ath_{df.name}"
        ] = (df_not_ath.sum().sum() / df_not_ath.shape[0])
        plot_df.append(ath_stat)
    plot_df = pd.concat(plot_df)
    plot_df.plot.bar(figsize=(15, 7), logy=True)


# %% [markdown] hidden=true
# ### Supporting variables

# %% run_control={"marked": false} hidden=true
# get the list of all coin paires for each exchange
binance_coins = imdauni.get_trade_universe("v0_1")["CCXT"]["binance"]
bitfinex_coins = imdauni.get_trade_universe("v0_1")["CCXT"]["bitfinex"]
ftx_coins = imdauni.get_trade_universe("v0_1")["CCXT"]["ftx"]
gateio_coins = imdauni.get_trade_universe("v0_1")["CCXT"]["gateio"]
kucoin_coins = imdauni.get_trade_universe("v0_1")["CCXT"]["kucoin"]

suffixes = {
    "ADA/USDT": "ada",
    "AVAX/USDT": "avax",
    "BNB/USDT": "bnb",
    "BTC/USDT": "btc",
    "DOGE/USDT": "doge",
    "EOS/USDT": "eos",
    "ETH/USDT": "eth",
    "LINK/USDT": "link",
    "SOL/USDT": "sol",
    "FIL/USDT": "fil",
    "XRP/USDT": "xrp",
}

# get the list of all unique coin names
coins = set(
    binance_coins + bitfinex_coins + ftx_coins + gateio_coins + kucoin_coins
)
coins = [i.split("/")[0].lower() for i in coins]

exch_names = ["binance", "bitfinex", "ftx", "gateio", "kucoin"]

# %% [markdown] hidden=true
# ## Load the volumes dataframes

# %% hidden=true
binance = get_volume_df_for_exch_notional(binance_coins, "binance")
bitfinex = get_volume_df_for_exch_notional(bitfinex_coins, "bitfinex")
ftx = get_volume_df_for_exch_notional(ftx_coins, "ftx")
gateio = get_volume_df_for_exch_notional(gateio_coins, "gateio")
kucoin = get_volume_df_for_exch_notional(kucoin_coins, "kucoin")

# %% hidden=true
binance = get_volume_df_for_exch(binance_coins, "binance")
bitfinex = get_volume_df_for_exch(bitfinex_coins, "bitfinex")
ftx = get_volume_df_for_exch(ftx_coins, "ftx")
gateio = get_volume_df_for_exch(gateio_coins, "gateio")
kucoin = get_volume_df_for_exch(kucoin_coins, "kucoin")

# %% hidden=true
binance.name = "binance"
bitfinex.name = "bitfinex"
ftx.name = "ftx"
gateio.name = "gateio"
kucoin.name = "kucoin"

exch_list = [binance, bitfinex, ftx, gateio, kucoin]

# %% [markdown] heading_collapsed=true
# # Compute total trading volume for each currency
#

# %% hidden=true
total_trading_vol = get_total_trading_volume_by_coins(coins, exch_list)

# %% hidden=true
total_trading_vol

# %% hidden=true
total_trading_vol["total_trading_volume_in_coins"].plot.bar(
    figsize=(15, 7), logy=True
)

# %% hidden=true
# daily_avg
total_trading_vol["daily_avg_coin_volume"].sort_values(ascending=False).plot.bar(
    figsize=(15, 7), logy=True
)

# %% [markdown] heading_collapsed=true
# # Rolling volume for each currency

# %% hidden=true
plot_rolling_volume_by_coins(coins, exch_list)

# %% [markdown] heading_collapsed=true
# # Compute total volume per exchange

# %% hidden=true
exchange_trading_volume = get_total_trading_volume_by_exchange(exch_list)

# %% hidden=true
exchange_trading_volume

# %% hidden=true
exchange_trading_volume["total_trading_volume_in_coins"].plot.bar(
    figsize=(15, 7), logy=True
)

# %% hidden=true
# normalised
exchange_trading_volume["daily_avg_coin_volume"].plot.bar(
    figsize=(15, 7), logy=True
)

# %% [markdown] heading_collapsed=true
# # Rolling volume for each exchange

# %% hidden=true
plot_rolling_volume_by_exchange(exch_list, exch_names)


# %% [markdown] heading_collapsed=true
# # Is volume constant over different days? E.g., weekend vs workdays?

# %% run_control={"marked": false} hidden=true
def compare_weekdays_volumes(exch_list):
    """
    Return statistics and graphs with working days vs.

    weekends analysis
    Parameters: volume dataframes
    """
    # clean the existing dataframes from previously calculated volumes
    df = pd.concat(exch_list, axis=1)
    df = df[[col for col in df.columns if "total_volume" not in col]]
    df = df[[col for col in df.columns if "rolling_volume" not in col]]
    # calculate new volumes that sum up all coins and exchanges
    df["total_volume"] = df.sum(axis=1)
    # create column with ids for weekdays
    df["weekday"] = df.index.map(lambda x: x.strftime("%A"))
    # plot total amount of volume for each day
    df.groupby("weekday").total_volume.sum().sort_values(
        ascending=False
    ).plot.bar(figsize=(12, 7))
    # plot working days vs. weekends
    weekends = df[(df["weekday"] == "Saturday") | (df["weekday"] == "Sunday")]
    sns.displot(weekends, x="total_volume")
    weekdays = df[(df["weekday"] != "Saturday") & (df["weekday"] != "Sunday")]
    sns.displot(weekdays, x="total_volume")
    # calculate descriptive statistics for working days vs. weekends
    print("Descriptive statistics:")
    weeknd_stat = weekends["total_volume"].describe()
    weekdys_stat = weekdays["total_volume"].describe()
    weeknd_stat = pd.DataFrame(weeknd_stat)
    weekdys_stat = pd.DataFrame(weekdys_stat)
    stats = pd.concat([weeknd_stat, weekdys_stat], axis=1)
    stats.columns = ["weekends", "working_days"]
    print(stats)
    print(
        "The graph labels in respective order: Total Volume by weekdays, Distribution of Volume over weekends, Distribution of Volume over working days"
    )


# %% hidden=true
compare_weekdays_volumes(exch_list)

# %% [markdown] heading_collapsed=true
# # How does it vary over hours? E.g., US stock times 9:30-16 vs other time

# %% [markdown] hidden=true
# ## Binance example

# %% hidden=true
binance_1 = get_initial_df_with_volumes(binance_coins, "binance")
bitfinex_1 = get_initial_df_with_volumes(bitfinex_coins, "bitfinex")
ftx_1 = get_initial_df_with_volumes(ftx_coins, "ftx")
gateio_1 = get_initial_df_with_volumes(gateio_coins, "gateio")
kucoin_1 = get_initial_df_with_volumes(kucoin_coins, "kucoin")

exchange_list = [binance_1, bitfinex_1, ftx_1, gateio_1, kucoin_1]
binance_1.name = "binance"
bitfinex_1.name = "bitfinex"
ftx_1.name = "ftx"
gateio_1.name = "gateio"
kucoin_1.name = "kucoin"

# %% hidden=true
plot_ath_volumes_comparison(exchange_list)

# %% hidden=true
