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
import im.data.universe as imdauni
import im.ccxt.data.load.loader as imccdaloloa

# %%
import pandas as pd
import numpy as np
import datetime as dt
import matplotlib.pyplot as plt
import seaborn as sns

# %%
pd.set_option('display.max_rows', 500)

# %% [markdown]
# ## Load the data

# %%
#imdauni.get_trade_universe()["CCXT"]

# %%
config = imccdaloloa.CcxtLoader(root_dir="s3://alphamatic-data/data", aws_profile="am")


# %% [markdown]
# ## Functions

# %%
def get_volume_df_for_exch(coins, exchange):
    result = []
    for coin in coins:
        df = config.read_data_from_filesystem(exchange_id=exchange,
                                               currency_pair=coin,
                                               data_type="OHLCV")
        volume_df = pd.DataFrame(df.groupby(by=df.index.date)["volume"].sum())
        volume_df.columns = [col_name+'_{x}_{y}'.format(x = suffixes[f"{coin}"], y=exchange) for col_name in volume_df.columns]
        result.append(volume_df)
    final_result = pd.concat(result, axis=1)
    return final_result

def total_trading_volume(coin_list):
    df = pd.concat([binance,bitfinex, ftx, gateio, kucoin],axis=1)
    volume_df = pd.DataFrame(columns=["total_trading_volume"])
    for coin in coin_list:
        coin_cols = [col for col in df.columns if coin in col]
        coin_df = df[coin_cols]
        coin_df.loc[:, ("total_volume")] = coin_df.sum(axis=1)
        total_volume_ = coin_df["total_volume"].sum()
        volume_df.loc["total_volume_{}".format(f"{coin}"), ("total_trading_volume")] = total_volume_
    return volume_df

def exch_trading_volume(df_list):
    exch_volume = pd.DataFrame(columns=["total_exchange_trading_volume"])
    for df in df_list:
        total_volume_ = df.sum().sum()
        exch_volume.loc["total_volume_{}".format(f"{df.name}"), "total_exchange_trading_volume"] = total_volume_
    return exch_volume

def rolling_volume_graph(coin_list):
    df = pd.concat([binance,bitfinex, ftx, gateio, kucoin],axis=1)
    for coin in coin_list:
        coin_df = df[[col for col in df.columns if coin in col]]
        coin_df.loc[:, ("total_volume")] = coin_df.sum(axis=1)
        coin_df.loc[:,('rolling_volume')] = coin_df['total_volume'].rolling(90).mean()
        #coin_df[["total_volume", "rolling_volume"]].plot(figsize=(15,7))
        plt.figure(figsize=(12,7))
        plt.title('{}'.format(coin))
        plt.xlabel('time')
        plt.ylabel('volume')
        plt.plot(coin_df[["total_volume", "rolling_volume"]])
        
def rolling_volume_graph_exch(exch_list):
    df = pd.concat([binance,bitfinex, ftx, gateio, kucoin],axis=1)
    for exch in exch_list:
        exch_cols = [col for col in df.columns if exch in col]
        exch_df = df[exch_cols]
        exch_df.loc[:, ("total_volume")] = exch_df.sum(axis=1)
        exch_df.loc[:,('rolling_volume')] = exch_df['total_volume'].rolling(90).mean()
        plt.figure(figsize=(12,7))
        plt.title('{}'.format(exch))
        plt.xlabel('time')
        plt.ylabel('volume')
        plt.plot(exch_df[["total_volume", "rolling_volume"]])
        
def weekday_analyzer():
    df = pd.concat([binance,bitfinex, ftx, gateio, kucoin],axis=1)
    df = df[[col for col in df.columns if "total_volume" not in col]]
    df = df[[col for col in df.columns if "rolling_volume" not in col]]
    df.loc[:, ("total_volume")] = df.sum(axis=1)
    df['weekday'] = df.index.map(lambda x: x.weekday())
    df.groupby("weekday").total_volume.sum().plot.bar(figsize=(12,7))   
    weekends = df[(df["weekday"] == 5)|(df["weekday"] == 6)]
    sns.displot(weekends, x="total_volume")
    weekdays = df[(df["weekday"] != 5)&(df["weekday"] != 6)]
    sns.displot(weekdays, x="total_volume")
    print("Descriptive statistics:")
    weeknd_stat = weekends["total_volume"].describe()
    weekdys_stat = weekdays["total_volume"].describe()
    weeknd_stat = pd.DataFrame(weeknd_stat)
    weekdys_stat = pd.DataFrame(weekdys_stat)
    stats = pd.concat([weeknd_stat,weekdys_stat], axis=1)
    stats.columns = ["weekends", "working_days"]
    print(stats)
    print("The graph labels in respective order: Total Volume by weekdays, Distribution of Volume over weekends, Distribution of Volume over working days")
    
def general_df_binance(coins, exchange):
    result = []
    for coin in coins:
        df = config.read_data_from_filesystem(exchange_id=exchange,
                                               currency_pair=coin,
                                               data_type="OHLCV")
        result.append(df["volume"])
    final_result = pd.concat(result, axis=1)
    return final_result

def ath_stats(df):
    df_ath = df.iloc[df.index.indexer_between_time("09:30", "16:00")]
    ath_index = df_ath.index
    df_not_ath = df.loc[~df.index.isin(ath_index)]
    ath_stat = pd.DataFrame(columns=["total_volume_ath", "total_volume_not_ath"])
    ath_stat.loc[0,"total_volume_ath"] = df_ath.sum().sum()
    ath_stat.loc[0,"total_volume_not_ath"] = df_not_ath.sum().sum()
    ath_stat.plot.bar()


# %% [markdown]
# ### Supporting variables

# %% run_control={"marked": false}
binance_coins = ['ADA/USDT',
                 'AVAX/USDT',
                 'BNB/USDT',
                 'BTC/USDT',
                 'DOGE/USDT',
                 'EOS/USDT',
                 'ETH/USDT',
                 'LINK/USDT',
                 'SOL/USDT']

bitfinex_coins = ['ADA/USDT',
                  'AVAX/USDT',
                  'BTC/USDT',
                  'DOGE/USDT',
                  'EOS/USDT',
                  'ETH/USDT',
                  'FIL/USDT',
                  'LINK/USDT',
                  'SOL/USDT',
                  'XRP/USDT']

ftx_coins = ['BNB/USDT',
             'BTC/USDT',
             'DOGE/USDT',
             'ETH/USDT',
             'LINK/USDT',
             'SOL/USDT',
             'XRP/USDT']

gateio_coins = ['ADA/USDT',
                'AVAX/USDT',
                'BNB/USDT',
                'BTC/USDT',
                'DOGE/USDT',
                'EOS/USDT',
                'ETH/USDT',
                'FIL/USDT',
                'LINK/USDT',
                'SOL/USDT',
                'XRP/USDT']

kucoin_coins = ['ADA/USDT',
                'AVAX/USDT',
                'BNB/USDT',
                'BTC/USDT',
                'DOGE/USDT',
                'EOS/USDT',
                'ETH/USDT',
                'FIL/USDT',
                'LINK/USDT',
                'SOL/USDT',
                'XRP/USDT']

suffixes = {'ADA/USDT': "ada",
            'AVAX/USDT': "avax",
            'BNB/USDT': "bnb",
            'BTC/USDT': "btc",
            'DOGE/USDT': "doge",
            'EOS/USDT': "eos",
            'ETH/USDT': 'eth',
            'LINK/USDT': "link",
            'SOL/USDT': "sol",
            'FIL/USDT':"fil",
            'XRP/USDT':"xrp"}

coins = ["ada",
         "avax",
         "bnb",
         "btc",
         "doge",
         "eos",
         "eth",
         "link",
         "sol",
         "fil",
         "xrp"]

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
binance.name = 'binance'
bitfinex.name = 'bitfinex'
ftx.name = 'ftx'
gateio.name = 'gateio'
kucoin.name = 'kucoin'

exch_list = [binance, bitfinex, ftx, gateio, kucoin]

# %% [markdown]
# # Compute total trading volume for each currency
#

# %%
total_trading_vol = total_trading_volume(coins)

# %%
total_trading_vol

# %%
total_trading_vol.plot.bar(figsize=(15,7))

# %%
# if we exclude DOGE
total_trading_vol[total_trading_vol.index!="total_volume_doge"].plot.bar(figsize=(15,7))

# %% [markdown]
# # Rolling volume for each currency

# %%
rolling_volume_graph(coins)

# %% [markdown]
# # Compute total volume per exchange

# %%
exchange_trading_volume = exch_trading_volume(exch_list)

# %%
exchange_trading_volume

# %%
# binance seems to attract the most attention in terms of trade volume
exchange_trading_volume.plot.bar(figsize=(15,7))

# %%
# if we exclude binance
exchange_trading_volume[exchange_trading_volume.index!="total_volume_binance"].plot.bar(figsize=(15,7))

# %% [markdown]
# # Rolling volume for each exchange

# %%
rolling_volume_graph_exch(exch_names)

# %% [markdown]
# # Is volume constant over different days? E.g., weekend vs workdays?

# %%
weekday_analyzer()

# %% [markdown]
# # How does it vary over hours? E.g., US stock times 9:30-16 vs other time

# %% [markdown]
# ## Binance example

# %%
binance_1 = general_df_binance(binance_coins, "binance")

# %%
ath_stats(binance_1)
