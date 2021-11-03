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
# ## Imports

# %%
import pandas as pd
import seaborn as sns

import im.ccxt.data.load.loader as imccdaloloa
import im.data.universe as imdauni

# %% [markdown]
# ## Configs

# %%
config = imccdaloloa.CcxtLoader(
    root_dir="s3://alphamatic-data/data", aws_profile="am"
)


# %% [markdown]
# ## Functions

# %% [markdown]
# Note: by "volume" I mean the standard output that is nominated in the number of coins

# %%
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
    volume_df = pd.DataFrame(columns=["total_trading_volume"])
    for coin in coin_list:
        coin_cols = [col for col in df.columns if coin in col]
        coin_df = df[coin_cols]
        coin_df["total_volume"] = coin_df.sum(axis=1)
        total_volume_ = coin_df["total_volume"].sum()
        norm_volume_ = coin_df["total_volume"].sum() / len(
            coin_df[coin_df["total_volume"] != 0]
        )
        volume_df.loc[
            "{}".format(f"{coin}"), ("total_trading_volume")
        ] = total_volume_
        volume_df.loc[
            "{}".format(f"{coin}"), ("normalised_total_volume")
        ] = norm_volume_
    return volume_df.sort_values(by="total_trading_volume", ascending=False)


def get_total_trading_volume_by_exchange(df_list):
    """
    Return the DataFrame with total trading volume on exchanges
    Parameters: volume dataframes
    """
    exch_volume = pd.DataFrame(columns=["total_exchange_trading_volume"])
    for df in df_list:
        total_volume_ = df.sum().sum()
        norm_volume_ = df.sum().sum() / df.shape[0]
        exch_volume.loc[
            "{}".format(f"{df.name}"), "total_exchange_trading_volume"
        ] = total_volume_
        exch_volume.loc[
            "{}".format(f"{df.name}"), "total_exchange_normalised_trading_volume"
        ] = norm_volume_
    return exch_volume.sort_values(
        by="total_exchange_trading_volume", ascending=False
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
    Return DataFrame with the volume of all coins for exhange with initial timestamps
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


def plot_ath_volumes_comparison(df):
    """
    Return the graph with the comparison of total trading volume in ATH vs.

    non-ATH
    Parameters: dataframe with volumes from a given exchange
    """
    df_ath = df.iloc[df.index.indexer_between_time("09:30", "16:00")]
    df_not_ath = df.loc[~df.index.isin(df_ath.index)]
    ath_stat = pd.DataFrame(columns=["total_volume_ath", "total_volume_not_ath"])
    ath_stat.loc[0, "total_volume_ath"] = df_ath.sum().sum()
    ath_stat.loc[0, "total_volume_not_ath"] = df_not_ath.sum().sum()
    ath_stat.plot.bar()


# %% [markdown]
# ### Supporting variables

# %% run_control={"marked": false}
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

# %% [markdown]
# ## Load the volumes dataframes

# %%
binance = get_volume_df_for_exch(binance_coins, "binance")
bitfinex = get_volume_df_for_exch(bitfinex_coins, "bitfinex")
ftx = get_volume_df_for_exch(ftx_coins, "ftx")
gateio = get_volume_df_for_exch(gateio_coins, "gateio")
kucoin = get_volume_df_for_exch(kucoin_coins, "kucoin")

# %%
binance.name = "binance"
bitfinex.name = "bitfinex"
ftx.name = "ftx"
gateio.name = "gateio"
kucoin.name = "kucoin"

exch_list = [binance, bitfinex, ftx, gateio, kucoin]

# %% [markdown]
# # Compute total trading volume for each currency
#

# %%
total_trading_vol = get_total_trading_volume_by_coins(coins, exch_list)

# %%
total_trading_vol

# %%
total_trading_vol["total_trading_volume"].plot.bar(figsize=(15, 7))

# %%
# normalised
total_trading_vol["normalised_total_volume"].plot.bar(figsize=(15, 7))

# %% [markdown]
# # Rolling volume for each currency

# %%
plot_rolling_volume_by_coins(coins, exch_list)

# %% [markdown]
# # Compute total volume per exchange

# %%
exchange_trading_volume = get_total_trading_volume_by_exchange(exch_list)

# %%
exchange_trading_volume

# %%
# binance seems to attract the most attention in terms of trade volume
exchange_trading_volume["total_exchange_trading_volume"].plot.bar(
    figsize=(15, 7), logy=True
)

# %%
# normalised
exchange_trading_volume["total_exchange_normalised_trading_volume"].plot.bar(
    figsize=(15, 7), logy=True
)

# %% [markdown]
# # Rolling volume for each exchange

# %%
plot_rolling_volume_by_exchange(exch_list, exch_names)

# %% [markdown]
# # Is volume constant over different days? E.g., weekend vs workdays?

# %%
compare_weekdays_volumes(exch_list)

# %% [markdown]
# # How does it vary over hours? E.g., US stock times 9:30-16 vs other time

# %% [markdown]
# ## Binance example

# %%
binance_1 = get_initial_df_with_volumes(binance_coins, "binance")

# %%
plot_ath_volumes_comparison(binance_1)
