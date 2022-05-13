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

# %%
# %load_ext autoreload
# %autoreload 2
# %matplotlib inline

# %% [markdown]
# # Imports

# %%
import logging
import os
import requests
import time

import ccxt
import matplotlib.pyplot as plt
import pandas as pd

import core.config.config_ as cconconf
import core.statistics as costatis
import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hprint as hprint
import helpers.hs3 as hs3
import helpers.hsecrets as hsecret
import im_v2.ccxt.data.client as icdcl
import im_v2.ccxt.data.extract.exchange_class as imvcdeexcl

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()


# %%
def get_cmtask1866_config_ccxt() -> cconconf.Config:
    """
    Get task1866-specific config.
    """
    config = cconconf.Config()
    # Load parameters.
    config.add_subconfig("load")
    config["load"]["aws_profile"] = "ck"
    #
    s3_bucket_path = hs3.get_s3_bucket_path(config["load"]["aws_profile"])
    s3_path = "s3://cryptokaizen-data/historical"
    config["load"]["data_dir"] = s3_path
    # Data parameters.
    config.add_subconfig("data")
    config["data"]["vendor"] = "CCXT"
    config["data"]["data_snapshot"] = "latest"
    config["data"]["version"] = "v3"
    config["data"]["resample_1min"] = True
    config["data"]["partition_mode"] = "by_year_month"
    config["data"]["start_ts"] = None
    config["data"]["end_ts"] = None
    config["data"]["columns"] = None
    config["data"]["filter_data_mode"] = "assert"
    # Column names.
    config.add_subconfig("column_names")
    config["column_names"]["full_symbol"] = "full_symbol"
    config["column_names"]["close_price"] = "close"
    return config


# %%
config = get_cmtask1866_config_ccxt()
print(config)

 # %%
 pd.set_option("display.float_format", "{:.8f}".format)


# %% [markdown]
# # Functions

# %%
def _get_qa_stats(data: pd.DataFrame, config: cconconf.Config) -> pd.DataFrame:
    """
    Get quality assurance stats per full symbol in data.
    """
    res_stats = []
    for full_symbol, symbol_data in data.groupby(
        config["column_names"]["full_symbol"]
    ):
        # Compute stats for a full symbol.
        symbol_stats = pd.Series(dtype="object", name=full_symbol)
        symbol_stats["min_timestamp"] = symbol_data.index.min()
        symbol_stats["max_timestamp"] = symbol_data.index.max()
        symbol_stats["NaNs %"] = 100 * (
            costatis.compute_frac_nan(
                symbol_data[config["column_names"]["close_price"]]
            )
        )
        symbol_stats["volume=0 %"] = 100 * (
            symbol_data[symbol_data["volume"] == 0].shape[0]
            / symbol_data.shape[0]
        )
        symbol_stats["bad data %"] = symbol_stats["NaNs %"] + symbol_stats["volume=0 %"]
        res_stats.append(symbol_stats)
    # Combine all full symbol stats.
    res_stats_df = pd.concat(res_stats, axis=1).T
    return res_stats_df


def _get_qa_stats_by_year_month(
    data: pd.DataFrame, config: cconconf.Config
) -> pd.DataFrame:
    """
    Get quality assurance stats per full symbol, year, and month.
    """
    #
    data["year"] = data.index.year
    data["month"] = data.index.month
    #
    res_stats = []
    columns_to_groupby = [config["column_names"]["full_symbol"], "year", "month"]
    for index, symbol_data in data.groupby(columns_to_groupby):
        #
        full_symbol, year, month = index
        # Get stats for a full symbol and add them to overall stats.
        symbol_stats = pd.Series(dtype="object", name=full_symbol)
        symbol_stats["year"] = year
        symbol_stats["month"] = month
        symbol_stats["NaNs %"] = 100 * (
            costatis.compute_frac_nan(
                symbol_data[config["column_names"]["close_price"]]
            )
        )
        symbol_stats["volume=0 %"] = 100 * (
            symbol_data[symbol_data["volume"] == 0].shape[0]
            / symbol_data.shape[0]
        )
        symbol_stats["bad data %"] = symbol_stats["NaNs %"] + symbol_stats["volume=0 %"]
        res_stats.append(symbol_stats)
    res_stats_df = pd.concat(res_stats, axis=1).T
    #
    res_stats_df["year"] = res_stats_df["year"].astype(int)
    res_stats_df["month"] = res_stats_df["month"].astype(int)
    # Set index by full symbol, year, and month.
    res_stats_df = res_stats_df.set_index([res_stats_df.index, "year", "month"])
    return res_stats_df


def _plot_bad_data_stats(bad_data_stats: pd.DataFrame) -> None:
    """
    Plot bad data stats per unique full symbol in data.
    """
    full_symbols = bad_data_stats.index.get_level_values(0).unique()
    for full_symbol in full_symbols:
        bad_data_col_name = "bad data %"
        _ = bad_data_stats.loc[full_symbol].plot.bar(
            y=bad_data_col_name, rot=0, title=full_symbol
        )


# %%
def set_index_ts(df):
    df["timestamp"] = df["timestamp"].apply(
        lambda x: hdateti.convert_unix_epoch_to_timestamp(x)
    )
    df = df.set_index("timestamp")
    return df


# %%
def percentage(df, df_loc):
    result = 100*len(df_loc)/len(df)
    return round(result, 2)

def log_into_exchange(exchange) -> ccxt.Exchange:
    """
    Log into an exchange via CCXT and return the corresponding
    `ccxt.Exchange` object.
    """
    # Select credentials for provided exchange.
    credentials = hsecret.get_secret(exchange)
    # Enable rate limit.
    credentials["rateLimit"] = True
    exchange_class = getattr(ccxt, exchange)
    # Create a CCXT Exchange class object.
    exchange = exchange_class(credentials)
    hdbg.dassert(
        exchange.checkRequiredCredentials(),
        msg="Required credentials not passed",
    )
    return exchange


# %%
def load_ccxt_data(currency_pair, since, exchange):
    ccxt_data = exchange.fetch_ohlcv(
            currency_pair,
            timeframe="1m",
            since=since,
            limit=500)
    columns = ["timestamp", "open", "high", "low", "close", "volume"]
    bars = pd.DataFrame(ccxt_data, columns=columns)
    return bars


# %%
def get_all_data(exchange, currency_pair, start_timestamp, end_timestamp):
    all_bars = []
    duration = exchange.parse_timeframe("1m") * 100
    for t in range(
            start_timestamp,
            end_timestamp + duration,
            duration * 500,
        ):
        bars = load_ccxt_data(currency_pair, t, exchange)
        all_bars.append(bars)
        time.sleep(1)
    return pd.concat(all_bars)


# %% [markdown]
# # CcxtHistoricalPqByTileClient

# %%
client = icdcl.CcxtHistoricalPqByTileClient(
    config["data"]["version"],
    config["data"]["resample_1min"],
    config["load"]["data_dir"],
    config["data"]["partition_mode"],
    aws_profile=config["load"]["aws_profile"],
)

# %%
universe = client.get_universe()
universe

# %% [markdown]
# # Binance::DOGE_USDT

# %%
binance_data = client.read_data(
    ["binance::DOGE_USDT"],
    config["data"]["start_ts"],
    config["data"]["end_ts"],
    config["data"]["columns"],
    config["data"]["filter_data_mode"],
)

# %%
binance_2019_09 = binance_data.loc[(binance_data.index.year == 2019) & (binance_data.index.month == 9)]
binance_2019_09_volume_0 = binance_2019_09.loc[binance_2019_09["volume"] == 0]

# %%
binance_2019_09

# %%
_LOG.info(binance_2019_09_volume_0.shape)
binance_2019_09_volume_0

# %% [markdown]
# # Extractor

# %%
ccxt_binance_DOGE_exchange = imvcdeexcl.CcxtExchange("binance")

# %%
sleep_time_in_secs = 1
start_timestamp = pd.Timestamp("2019-09-01 00:00:00+00:00")
end_timestamp = pd.Timestamp("2019-09-30 23:59:59+00:00")
ccxt_binance_DOGE = ccxt_binance_DOGE_exchange.download_ohlcv_data(
    "DOGE/USDT",
    start_timestamp=start_timestamp,
    end_timestamp=end_timestamp,
    sleep_time_in_secs=sleep_time_in_secs,
)

# %%
ccxt_binance_DOGE = set_index_ts(ccxt_binance_DOGE)

# %%
ccxt_binance_DOGE = ccxt_binance_DOGE.loc[ccxt_binance_DOGE.index.month == 9]

# %%
ccxt_binance_DOGE.loc[ccxt_binance_DOGE['volume'] == 0]

# %%
ccxt_binance_DOGE

# %% [markdown]
# Where`volume = 0`, data from columns `open`, `high`, `low`, `close` is exactly the same from previous row where `volume != 0`. It could mean that `volume = 0` rows are `NaNs` at the source, so it could be the way exchange handles missing data.

# %%
print(percentage(ccxt_binance_DOGE, ccxt_binance_DOGE.loc[ccxt_binance_DOGE['volume'] == 0]))

# %% [markdown]
# # CCXT w/o Extractor

# %%
ccxt_exchange = log_into_exchange('binance')

# %%
ccxt_df = get_all_data(ccxt_exchange, "DOGE/USDT", 1567296000000, 1569887999000)

# %%
ccxt_df = set_index_ts(ccxt_df)
ccxt_df.index.min(), ccxt_df.index.max(), ccxt_df.shape

# %%
ccxt_df = ccxt_df.loc[ccxt_df.index.month == 9]

# %%
ccxt_df.isna().value_counts()

# %%
ccxt_df.loc[ccxt_df['volume'] != 0]

# %% [markdown]
# # Summary

# %% [markdown]
#
# |CCXT | | ||			Extractor	| | | |Client | | |
# |------|--|-||-------------|-|-|-|------|-|-|
# |date|Number of NaN rows %|	Total number of rows| `volume=0` %	|Number of NaN rows %|	Total number of rows| `volume=0` %| Number of NaN rows %|	Total number of rows| `volume=0` %|
# |2019-09|	0          |	                   429750|	      73.22%   	|	0          |	                   43200|	      73.3%   |      0|	            43200| 73.3%|
#

# %% [markdown]
# - The huge amount of data from CCXT is duplicates. Unique values are 43200.
# - Where volume = 0, data from columns open, high, low, close is exactly the same from previous row where volume != 0. It could mean that volume = 0 rows are NaNs at the source, so it could be the way exchange handles missing data.

# %% [markdown]
# # ftx::BTC_USDT

# %% [markdown]
# ## Client

# %%
ftx_data = client.read_data(
    ["ftx::BTC_USDT"],
    config["data"]["start_ts"],
    config["data"]["end_ts"],
    config["data"]["columns"],
    config["data"]["filter_data_mode"],
)

# %%
ftx_2020_04 = ftx_data.loc[(ftx_data.index.year == 2020) & (ftx_data.index.month == 4)]
ftx_2020_04_volume_0 = ftx_2020_04.loc[ftx_2020_04["volume"] == 0]
ftx_2020_04_volume_0

# %%
ftx_2020_04

# %%
ftx_2020_04.loc[ftx_2020_04['open'].isna()]

# %%
print(percentage(ftx_2020_04, ftx_2020_04_volume_0))

# %% [markdown]
# ## Extractor

# %%
ccxt_ftx_BTC_exchange = imvcdeexcl.CcxtExchange("ftx")
sleep_time_in_secs = 1
start_timestamp = pd.Timestamp("2020-04-01 00:00:00+00:00")
end_timestamp = pd.Timestamp("2020-04-30 23:59:59+00:00")
ccxt_ftx_BTC = ccxt_ftx_BTC_exchange.download_ohlcv_data(
    "BTC/USDT",
    start_timestamp=start_timestamp,
    end_timestamp=end_timestamp,
    sleep_time_in_secs=sleep_time_in_secs,
)

# %%
ccxt_ftx_BTC = set_index_ts(ccxt_ftx_BTC)


# %%
ccxt_ftx_BTC = ccxt_ftx_BTC.loc[ccxt_ftx_BTC.index.month == 4]

# %%
ccxt_ftx_BTC.loc[ccxt_ftx_BTC['volume'] == 0]

# %%
ccxt_ftx_BTC

# %%
ccxt_ftx_BTC.loc[(ccxt_ftx_BTC['high'] == 7493.50000000)
                 & (ccxt_ftx_BTC['volume'] == 0)]

# %%
ccxt_ftx_BTC.loc[(ccxt_ftx_BTC.index.day == 25)
                 & (ccxt_ftx_BTC.index.hour == 3)]

# %% [markdown]
# So far `ftx` doesn't have same pattern as `binance` where `volume=0` rows have values from the last non-`volume=0` row.

# %%
print(percentage(ccxt_ftx_BTC, ccxt_ftx_BTC.loc[ccxt_ftx_BTC['volume'] == 0]))

# %% [markdown]
# ## CCXT w/o Extractor

# %%
ccxt_exchange_ftx = log_into_exchange('ftx')
ccxt_df_ftx = get_all_data(ccxt_exchange_ftx, "BTC/USDT", 1585699200000, 1588291199000)
ccxt_df_ftx = set_index_ts(ccxt_df_ftx)
ccxt_df_ftx.index.min(), ccxt_df_ftx.index.max(), ccxt_df_ftx.shape

# %%
ccxt_df_ftx = ccxt_df_ftx.loc[ccxt_df_ftx.index.month == 4]

# %%
ccxt_df_ftx.isna().value_counts()

# %%
len(ccxt_df_ftx.index.unique())

# %%
ccxt_df_ftx

# %%
print(percentage(ccxt_df_ftx, ccxt_df_ftx.loc[ccxt_df_ftx['volume'] == 0]))

# %% [markdown]
#
# |CCXT | | ||			Extractor	| | | |Client | | |
# |------|--|-||-------------|-|-|-|------|-|-|
# |date|Number of NaN rows %|	Total number of rows| `volume=0` %	|Number of NaN rows %|	Total number of rows| `volume=0` %| Number of NaN rows %|	Total number of rows| `volume=0` %|
# |2019-09|	0          |	                   429750|	      86.09%   	|	0          |	                   43200|	      85.97%   |      0|	            43200| 85.97%|
#

# %%
