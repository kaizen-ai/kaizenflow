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
# ## Imports

# %%
import logging

import pandas as pd

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hprint as hprint
import im_v2.ccxt.data.client.ccxt_clients as imvcdccccl
import im_v2.talos.data.client.talos_clients as imvtdctacl
import im_v2.talos.data.extract.exchange_class as imvtdeexcl

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

hprint.config_notebook()


# %% [markdown]
# ## Functions

# %%
def get_data_from_talos_db(start_time, end_time):
    # Set start and end dates.
    start_timestamp = pd.Timestamp(start_time)
    end_timestamp = pd.Timestamp(end_time)

    # Load the data.
    df = talos_extract.download_ohlcv_data(
        currency_pair="BTC-USD",
        exchange="binance",
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
        bar_per_iteration=100,
    )
    df["timestamp"] = df["timestamp"].apply(
        lambda x: hdateti.convert_unix_epoch_to_timestamp(x)
    )
    return df


def get_data_from_ccxt_client(start_time, end_time):
    # Specify the params.
    full_symbol_binance = "binance::BTC_USDT"
    start_time = pd.to_datetime(start_time)
    end_time = pd.to_datetime(end_time)
    df = ccxt_client._read_data_for_one_symbol(
        full_symbol_binance, start_time, end_time
    )
    return df


# %% [markdown]
# # Talos DB

# %%
# Initialize extractor.
talos_extract = imvtdeexcl.TalosExchange("sandbox")

# %%
data_talos_db = get_data_from_talos_db(
    "2022-01-01T10:00:24.000000Z", "2022-01-01T10:08:00.000000Z"
)
display(data_talos_db.head(3))
display(data_talos_db.tail(3))

# %% [markdown]
# ### Talos query summary

# %% [markdown]
# Beginning
# - If proposing query for __a complete minute__ (e.g., __10:00:00__) - it starts with __exactly mentioned timestamp__ (i.e., __10:00:00__).
# - If proposing query for __an incomplete minute__ (e.g., __10:00:36 or 10:00:24__) - it starts with __mentioned timestamp + 1min__ (i.e., __10:01:00__).
#    - Since the ohlcv output is blank (equal to zero), it's hard to understand whether volume or prices data changes during incomplete minute query.
#
# End
# - If proposing query for __a complete minute__ (e.g., __10:07:00__) - it starts with __exactly mentioned timestamp - 1min__ (i.e., __10:06:00__).
# - If proposing query for __an incomplete minute__ (e.g., __10:07:36 or 10:07:24__) - it starts with __exactly mentioned timestamp__ (i.e., __10:07:00__).
# - If proposing query for __previous minute + 1min__ (e.g., __10:08:00__) - it starts with __exactly mentioned timestamp - 1min__ (i.e., __10:07:00__).

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
    vendor, True, root_dir, extension, aws_profile=aws_profile_ccxt
)

# %% run_control={"marked": false}
data_ccxt_client = get_data_from_ccxt_client(
    "2020-01-01 10:00:02", "2020-01-01 10:08:00"
)

# %%
display(data_ccxt_client.head(3))
display(data_ccxt_client.tail(3))

# %% [markdown]
# ### Current CCXT client summary

# %% [markdown]
# Beginning
# - If proposing query for __a complete minute__ (e.g., __10:00:00__) - it starts with __exactly mentioned timestamp__ (i.e., __10:00:00+00:00__).
# - If proposing query for __an incomplete minute__ (e.g., __10:00:36 or 10:00:24__) - it starts with __mentioned timestamp + 1min__ (i.e., __10:01:00__).
#    - - Since the ohlcv output is available, one can check through volume or prices data that changing the query within a minute (e.g., 10:00:02 or 10:00:45) doesn't affect the numbers, so it means that the timestamp indicates the end of time period.
#
# End
# - If proposing query for __a complete minute__ (e.g., __10:07:00__) - it starts with __exactly mentioned timestamp__ (i.e., 10:07:00).
# - If proposing query for __an incomplete minute__ (e.g., __10:07:36 or 10:07:24__) - it starts with __exactly mentioned timestamp__ (i.e., __10:07:00__).
# - If proposing query for __previous minute + 1min__ (e.g., __10:08:00__) - it starts with __exactly mentioned timestamp__ (i.e., __10:08:00__).

# %% [markdown]
# # Current implemented Talos client

# %%
# Initialize Talos client.
import im_v2.im_lib_tasks as imvimlita
import helpers.hsql as hsql

env_file = imvimlita.get_db_env_path("dev")
connection_params = hsql.get_connection_info_from_env_file(env_file)
connection = hsql.get_connection(*connection_params)
table_name = "talos_ohlcv"
talos_client = imvtdctacl.RealTimeSqlTalosClient(
    True, connection, table_name
)
full_symbols = ["binance::BTC_USDT"]
df = talos_client.read_data(
     full_symbols,
     start_ts=pd.Timestamp('2022-03-16 22:47:50+0000'),
     end_ts=pd.Timestamp('2022-03-16 22:54:00+0000'),
)
display(df.head(3))
display(df.tail(3))


# %% [markdown]
# ### Talos client summary

# %% [markdown]
# Beginning
# - If proposing query for __a complete minute__ (e.g., __10:00:00__) - it starts with __exactly mentioned timestamp__ (i.e., __10:00:00__).
# - If proposing query for __an incomplete minute__ (e.g., __10:00:36 or 10:00:24__) - it starts with __mentioned timestamp + 1min__ (i.e., __10:01:00__).
#    - Since the ohlcv output is available, one can check through volume or prices data that changing the query within a minute (e.g., 10:00:02 or 10:00:45) doesn't affect the numbers, so it means that the timestamp indicates end of time period.
#
# End
# - If proposing query for __a complete minute__ (e.g., __10:07:00__) - it starts with __exactly mentioned timestamp__ (i.e., __10:07:00__).
# - If proposing query for __an incomplete minute__ (e.g., __10:07:36 or 10:07:24__) - it starts with __exactly mentioned timestamp__ (i.e., __10:07:00__).
# - If proposing query for __previous minute +1min__ (e.g., __10:08:00__) - it starts with __exactly mentioned timestamp__ (i.e., __10:08:00__).