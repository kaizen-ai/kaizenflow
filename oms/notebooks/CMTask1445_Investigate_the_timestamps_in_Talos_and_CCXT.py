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

# %% [markdown]
# ## Imports

# %%
import pandas as pd
import im_v2.talos.data.client.talos_clients as imvtdctacl
import im_v2.talos.data.extract.exchange_class as imvtdeexcl
import helpers.hdatetime as hdateti
import im_v2.ccxt.data.client.ccxt_clients as imvcdccccl

import helpers.hdbg as hdbg
import helpers.hprint as hprint
import logging
import logging

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

hprint.config_notebook()


# %% [markdown]
# ## Functions

# %%
def get_data_from_talos_query(start_time, end_time):
    # Set start and end dates.
    start_timestamp = pd.Timestamp(start_time)
    end_timestamp= pd.Timestamp(end_time)

    # Load the data.
    df = talos_extract.download_ohlcv_data(
            currency_pair = "BTC-USD",
            exchange = "binance",
            start_timestamp = start_timestamp,
            end_timestamp = end_timestamp,
            bar_per_iteration = 100,
        )
    df["timestamp"] = df["timestamp"].apply(lambda x: hdateti.convert_unix_epoch_to_timestamp(x))
    return df

def get_data_from_ccxt_client(start_time, end_time):
    # Specify the params.
    full_symbol_binance = "binance::BTC_USDT"
    start_time = pd.to_datetime(start_time)
    end_time = pd.to_datetime(end_time)
    df = ccxt_client._read_data_for_one_symbol(full_symbol_binance, start_time, end_time)
    return df

def get_data_from_talos_client(start_time, end_time):
    # Specify the params.
    full_symbol_binance = "binance::BTC_USDT"
    start_time = pd.to_datetime(start_time)
    end_time = pd.to_datetime(end_time)
    # Load the data.
    df = talos_client._read_data_for_one_symbol(full_symbol_binance, start_time, end_time)
    return df


# %% [markdown]
# # Talos query

# %%
# Initialize extractor.
talos_extract = imvtdeexcl.TalosExchange("sandbox")

# %%
data_talos_query = get_data_from_talos_query("2022-01-01T10:00:45.000000Z", "2022-01-01T10:07:15.000000Z")
data_talos_query.head(3)

# %% [markdown]
# # Current CCXT client

# %%
# Specify the params.
vendor = "CCXT"
root_dir = "s3://alphamatic-data/data"
extension = "csv.gz"
aws_profile_ccxt = "am"
# Initialize CCXT client.
ccxt_client = imvcdccccl.CcxtCddCsvParquetByAssetClient(
    vendor,
    root_dir,
    extension,
    aws_profile = aws_profile_ccxt
)

# %% run_control={"marked": false}
data_ccxt_client = get_data_from_ccxt_client("2020-01-01 10:00:45","2020-01-01 10:07:15")

# %%
data_ccxt_client

# %% [markdown]
# # Current implemented Talos client

# %%
# Initialize Talos client.
root_dir_talos = "s3://cryptokaizen-data/historical"
aws_profile_talos = "ck"
talos_client = imvtdctacl.TalosParquetByTileClient(root_dir_talos, aws_profile=aws_profile_talos)

# %%
data_talos_client = get_data_from_talos_client("2022-01-01 10:00:45", "2022-01-01 10:07:15")
data_talos_client.head(3)

# %%
