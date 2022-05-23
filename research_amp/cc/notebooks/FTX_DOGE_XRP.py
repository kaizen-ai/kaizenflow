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
import logging
import os
import json

import pandas as pd
import requests

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hs3 as hs3
import im_v2.crypto_chassis.data.client.crypto_chassis_clients as ivccdcccc

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)


# %%
def load_crypto_chassis_ohlcv(exhange_id, currency_pair):
    r = requests.get(
        f"https://api.cryptochassis.com/v1/ohlc/{exhange_id}/{currency_pair}?startTime=0"
    )
    df = pd.read_csv(r.json()["historical"]["urls"][0]["url"], compression="gzip")
    df["time_seconds"] = df["time_seconds"].apply(
        lambda x: hdateti.convert_unix_epoch_to_timestamp(x, unit="s")
    )
    df = df.set_index("time_seconds")
    return df


# %%
# Load data from CryptoChassis API.
ftx_xrp = load_crypto_chassis_ohlcv("ftx", "xrp-usdt")
ftx_xrp_2022_4 = ftx_xrp.loc[(ftx_xrp.index.year == 2022) & (ftx_xrp.index.month == 4)]
ftx_xrp_2022_4.shape

# %%
# Load data from S3.
universe_version = "v1"
resample_1min = True
root_dir = os.path.join(
                    hs3.get_s3_bucket_path("ck"), 
                    "reorg", "historical.manual.pq",
)
partition_mode = "by_year_month"
client = ivccdcccc.CryptoChassisHistoricalPqByTileClient(
        universe_version,
        resample_1min,
        root_dir,
        partition_mode,
        aws_profile="ck"
)

# %%
start_ts = None
end_ts = None
columns = None
filter_data_mode = "warn"
full_symbols = ["ftx::XRP_USDT", "ftx::DOGE_USDT"]
df = client.read_data(full_symbols, start_ts, end_ts, columns, filter_data_mode)

# %%
df_xrp = df.loc[df['full_symbol'] == full_symbols[0]]
df_xrp_2022_04 = df_xrp.loc[(df_xrp.index.year == 2022) 
                            & (df_xrp.index.month == 4) 
                            & (df_xrp['open'].isna() == False)]
len(df_xrp_2022_04) == len(ftx_xrp_2022_4)

# %%
# Load data from CryptoChassis API.
ftx_doge = load_crypto_chassis_ohlcv("ftx", "doge-usdt")
ftx_doge_2022_3 = ftx_doge.loc[(ftx_doge.index.year == 2022) & (ftx_doge.index.month == 3)]
ftx_doge_2022_3.shape

# %%
df_doge = df.loc[df['full_symbol'] == full_symbols[1]]
df_doge_2022_3 = df_doge.loc[(df_doge.index.year == 2022) 
                            & (df_doge.index.month == 3) 
                            & (df_doge['open'].isna() == False)]
len(df_doge_2022_3) == len(ftx_doge_2022_3)

# %%
df1 = df_doge.loc[(df_doge.index.year == 2022) 
                            & (df_doge.index.month == 3) 
                            & (df_doge['open'].isna() == True)]
df1.head(3)

# %%
ftx_doge_2022_3.loc[ftx_doge_2022_3.index.day == 1][23:30]

# %% [markdown]
# Data with NaNs on S3 is absent at the source.
