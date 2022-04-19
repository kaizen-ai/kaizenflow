# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.13.7
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown] heading_collapsed=true
# # Imports

# %% hidden=true
import logging
import os

import pandas as pd

import core.config.config_ as cconconf
import core.statistics as cstats
import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hpandas as hpandas
import helpers.hparquet as hparque
import helpers.hprint as hprint

# %% hidden=true
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()


# %% [markdown]
# # Configs

# %%
def get_cmtask1680_config_ccxt() -> cconconf.Config:
    """
    Get task1680-specific config.
    """
    config = cconconf.Config()
    # Load parameters.
    config.add_subconfig("load")
    config["load"]["aws_profile"] = "ck"
    # TODO(Nina): replace `s3://cryptokaizen-data` on get_s3_bucket() after #1667 is implemented.
    config["load"]["data_dir"] = os.path.join(
        "s3://cryptokaizen-data", "daily_staged"
    )
    # Data parameters.
    config.add_subconfig("data")
    config["data"]["vendor"] = "CCXT"
    # Column names.
    config.add_subconfig("column_names")
    config["column_names"]["close_price"] = "close"
    config["column_names"]["currency_pair"] = "currency_pair"
    config["column_names"]["exchange_id"] = "exchange_id"
    return config


# %%
config = get_cmtask1680_config_ccxt()
print(config)


# %% [markdown]
# # Functions

# %%
def compute_currency_pair_data_stats(currency_pair_list: list) -> pd.DataFrame:
    res = {}
    for currency_pair in currency_pair_list:
        data_currency_pair = data.loc[data["currency_pair"] == currency_pair]
        days_availiable = (
            data_currency_pair.last_valid_index()
            - data_currency_pair.first_valid_index()
        ).days
        n_data_points = data_currency_pair.close.count()
        coverage = 100 * (1 - cstats.compute_frac_nan(data_currency_pair.close))
        res.update(
            {
                currency_pair: [
                    data_currency_pair.index.min(),
                    data_currency_pair.index.max(),
                    n_data_points,
                    coverage,
                    days_availiable,
                    n_data_points / days_availiable,
                ]
            }
        )
        df_res = pd.DataFrame(
            data=res.values(),
            columns=[
                "min_ts",
                "max_ts",
                "n_data_points",
                "coverage",
                "days_available",
                "avg_data_points_per_day",
            ],
            index=res.keys(),
        )
    return df_res


# %%
def read_exchange_df(paths: list) -> pd.DataFrame:
    all_data = []
    for currency_pair, path in paths:
        data = hpandas.read_csv_to_df(path)
        data["currency_pair"] = currency_pair
        all_data.append(data)
    df = pd.concat(all_data)
    return df


# %%
def set_datetime_index(timestamps):
    times = []
    for time in timestamps:
        times.append(hdateti.convert_unix_epoch_to_timestamp(time))
    return times


# %% [markdown]
# # Load CCXT at cryptokaizen-data/historical/

# %% [markdown]
# ## binance stat

# %%
file_path = "s3://cryptokaizen-data/historical/ccxt/latest/binance/"
kwargs = {"aws_profile": "ck"}
data = hparque.from_parquet(file_path, **kwargs)
data.head()

# %%
currency_pairs = list(data["currency_pair"].unique())
dfb = compute_currency_pair_data_stats(currency_pairs)
dfb["exchange_id"] = "binance"
dfb["vendor"] = config["data"]["vendor"]
dfb

# %% [markdown] heading_collapsed=true
# ## bitfinex stat

# %% hidden=true
file_path = "s3://cryptokaizen-data/historical/ccxt/latest/bitfinex/"
kwargs = {"aws_profile": "ck"}
data = hparque.from_parquet(file_path, **kwargs)
data.head()

# %% hidden=true
currency_pairs = list(data["currency_pair"].unique())
dfb = compute_currency_pair_data_stats(currency_pairs)
dfb["exchange_id"] = "bitfinex"
dfb["vendor"] = config["data"]["vendor"]
dfb

# %% [markdown] heading_collapsed=true
# ## ftx stat

# %% hidden=true
file_path = "s3://cryptokaizen-data/historical/ccxt/latest/ftx/"
kwargs = {"aws_profile": "ck"}
data = hparque.from_parquet(file_path, **kwargs)
data.head()

# %% hidden=true
currency_pairs = list(data["currency_pair"].unique())
dfb = compute_currency_pair_data_stats(currency_pairs)
dfb["exchange_id"] = "ftx"
dfb["vendor"] = config["data"]["vendor"]
dfb

# %% [markdown] heading_collapsed=true
# ## gateio stat

# %% hidden=true
file_path = "s3://cryptokaizen-data/historical/ccxt/latest/gateio/"
kwargs = {"aws_profile": "ck"}
data = hparque.from_parquet(file_path, **kwargs)
data.head()

# %% hidden=true
currency_pairs = list(data["currency_pair"].unique())
dfb = compute_currency_pair_data_stats(currency_pairs)
dfb["exchange_id"] = "gateio"
dfb["vendor"] = config["data"]["vendor"]
dfb

# %% [markdown]
# ## kucoin stat

# %%
file_path = "s3://cryptokaizen-data/historical/ccxt/latest/kucoin/"
kwargs = {"aws_profile": "ck"}
data = hparque.from_parquet(file_path, **kwargs)
data.head()

# %%
currency_pairs = list(data["currency_pair"].unique())
dfb = compute_currency_pair_data_stats(currency_pairs)
dfb["exchange_id"] = "kucoin"
dfb["vendor"] = config["data"]["vendor"]
dfb

# %% [markdown] heading_collapsed=true
# # Load CCXT at cryptokaizen-data2/historical/

# %% [markdown] hidden=true
# ## binance stat

# %% hidden=true
paths = [
    (
        "ADA_USDT",
        "s3://cryptokaizen-data2/historical/binance/ADA_USDT_20220210-104334.csv",
    ),
    (
        "AVAX_USDT",
        "s3://cryptokaizen-data2/historical/binance/AVAX_USDT_20220210-105623.csv",
    ),
    (
        "BNB_USDT",
        "s3://cryptokaizen-data2/historical/binance/BNB_USDT_20220210-110910.csv",
    ),
    (
        "BTC_USDT",
        "s3://cryptokaizen-data2/historical/binance/BTC_USDT_20220210-112208.csv",
    ),
    (
        "DOGE_USDT",
        "s3://cryptokaizen-data2/historical/binance/DOGE_USDT_20220210-113502.csv",
    ),
    (
        "EOS_USDT",
        "s3://cryptokaizen-data2/historical/binance/EOS_USDT_20220210-114748.csv",
    ),
    (
        "ETH_USDT",
        "s3://cryptokaizen-data2/historical/binance/ETH_USDT_20220210-120031.csv",
    ),
    (
        "LINK_USDT",
        "s3://cryptokaizen-data2/historical/binance/LINK_USDT_20220210-121311.csv",
    ),
    (
        "SOL_USDT",
        "s3://cryptokaizen-data2/historical/binance/SOL_USDT_20220210-122551.csv",
    ),
]
data = read_exchange_df(paths)
data.head()

# %% hidden=true
hdateti.convert_unix_epoch_to_timestamp(data.timestamp.max())

# %% hidden=true
hdateti.convert_unix_epoch_to_timestamp(data.timestamp.min())

# %% [markdown] hidden=true
# ## bitfinex stat

# %% hidden=true
paths = [
    (
        "ADA_USDT",
        "s3://cryptokaizen-data2/historical/bitfinex/ADA_USDT_20220211-161045.csv",
    ),
    (
        "AVAX_USDT",
        "s3://cryptokaizen-data2/historical/bitfinex/AVAX_USDT_20220211-161212.csv",
    ),
    (
        "BTC_USDT",
        "s3://cryptokaizen-data2/historical/bitfinex/BTC_USDT_20220211-161338.csv",
    ),
    (
        "DOGE_USDT",
        "s3://cryptokaizen-data2/historical/bitfinex/DOGE_USDT_20220211-161507.csv",
    ),
    (
        "EOS_USDT",
        "s3://cryptokaizen-data2/historical/bitfinex/EOS_USDT_20220211-161634.csv",
    ),
    (
        "ETH_USDT",
        "s3://cryptokaizen-data2/historical/bitfinex/ETH_USDT_20220211-161801.csv",
    ),
    (
        "FIL_USDT",
        "s3://cryptokaizen-data2/historical/bitfinex/FIL_USDT_20220211-161926.csv",
    ),
    (
        "LINK_USDT",
        "s3://cryptokaizen-data2/historical/bitfinex/LINK_USDT_20220211-162053.csv",
    ),
    (
        "SOL_USDT",
        "s3://cryptokaizen-data2/historical/bitfinex/SOL_USDT_20220211-162219.csv",
    ),
    (
        "XRP_USDT",
        "s3://cryptokaizen-data2/historical/bitfinex/XRP_USDT_20220211-162345.csv",
    ),
]
data = read_exchange_df(paths)
data.head()

# %% hidden=true
hdateti.convert_unix_epoch_to_timestamp(data.timestamp.max())

# %% hidden=true
hdateti.convert_unix_epoch_to_timestamp(data.timestamp.min())

# %% [markdown] hidden=true
# ## ftx stat

# %% hidden=true
paths = [
    (
        "BNB_USDT",
        "s3://cryptokaizen-data2/historical/ftx/BNB_USDT_20220210-104642.csv",
    ),
    (
        "BNB_USDT",
        "s3://cryptokaizen-data2/historical/ftx/BNB_USDT_20220210-123958.csv",
    ),
    (
        "BTC_USDT",
        "s3://cryptokaizen-data2/historical/ftx/BTC_USDT_20220210-110047.csv",
    ),
    (
        "BTC_USDT",
        "s3://cryptokaizen-data2/historical/ftx/BTC_USDT_20220210-125404.csv",
    ),
    (
        "DOGE_USDT",
        "s3://cryptokaizen-data2/historical/ftx/DOGE_USDT_20220210-111452.csv",
    ),
    (
        "ETH_USDT",
        "s3://cryptokaizen-data2/historical/ftx/ETH_USDT_20220210-112851.csv",
    ),
    (
        "LINK_USDT",
        "s3://cryptokaizen-data2/historical/ftx/LINK_USDT_20220210-114240.csv",
    ),
    (
        "SOL_USDT",
        "s3://cryptokaizen-data2/historical/ftx/SOL_USDT_20220210-115701.csv",
    ),
    (
        "XRP_USDT",
        "s3://cryptokaizen-data2/historical/ftx/XRP_USDT_20220210-121122.csv",
    ),
]
data = read_exchange_df(paths)
data.head()

# %% hidden=true
hdateti.convert_unix_epoch_to_timestamp(data.timestamp.max())

# %% hidden=true
hdateti.convert_unix_epoch_to_timestamp(data.timestamp.min())

# %% [markdown] hidden=true
# ## gateio stat

# %% hidden=true
paths = [
    (
        "BNB_USDT",
        "s3://cryptokaizen-data2/historical/gateio/ADA_USDT_20220210-112115.csv",
    ),
    (
        "AVAX_USDT",
        "s3://cryptokaizen-data2/historical/gateio/AVAX_USDT_20220210-113306.csv",
    ),
    (
        "BNB_USDT",
        "s3://cryptokaizen-data2/historical/gateio/BNB_USDT_20220210-114500.csv",
    ),
    (
        "BTC_USDT",
        "s3://cryptokaizen-data2/historical/gateio/BTC_USDT_20220210-115659.csv",
    ),
    (
        "DOGE_USDT",
        "s3://cryptokaizen-data2/historical/gateio/DOGE_USDT_20220210-120851.csv",
    ),
    (
        "EOS_USDT",
        "s3://cryptokaizen-data2/historical/gateio/EOS_USDT_20220210-122048.csv",
    ),
    (
        "ETH_USDT",
        "s3://cryptokaizen-data2/historical/gateio/ETH_USDT_20220210-123244.csv",
    ),
    (
        "FIL_USDT",
        "s3://cryptokaizen-data2/historical/gateio/FIL_USDT_20220210-124438.csv",
    ),
    (
        "LINK_USDT",
        "s3://cryptokaizen-data2/historical/gateio/LINK_USDT_20220210-125629.csv",
    ),
    (
        "SOL_USDT",
        "s3://cryptokaizen-data2/historical/gateio/SOL_USDT_20220210-130821.csv",
    ),
    (
        "XRP_USDT",
        "s3://cryptokaizen-data2/historical/gateio/XRP_USDT_20220210-132013.csv",
    ),
]
data = read_exchange_df(paths)
data.head()

# %% hidden=true
hdateti.convert_unix_epoch_to_timestamp(data.timestamp.max())

# %% hidden=true
hdateti.convert_unix_epoch_to_timestamp(data.timestamp.min())

# %% [markdown] heading_collapsed=true
# # Load CCXT at cryptokaizen-data/daily_staged

# %% hidden=true
file_path = "%s/ccxt/binance/" % config["load"]["data_dir"]
kwargs = {"aws_profile": "ck"}
data = hparque.from_parquet(file_path, **kwargs)
data.head()

# %% [markdown] hidden=true
# ## Calculate stats

# %% hidden=true
currency_pairs = list(data["currency_pair"].unique())

# %% hidden=true
dfb = compute_currency_pair_data_stats(currency_pairs)
dfb["exchange_id"] = "binance"
dfb["vendor"] = config["data"]["vendor"]
dfb
