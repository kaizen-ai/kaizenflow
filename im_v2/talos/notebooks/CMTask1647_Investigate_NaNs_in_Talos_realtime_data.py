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
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# %%
import logging

import pandas as pd

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hprint as hprint
import im_v2.ccxt.data.client.ccxt_clients as imvcdccccl
import im_v2.talos.data.client.talos_clients as imvtdctacl
import im_v2.talos.data.extract.exchange_class as imvtdeexcl

import im_v2.im_lib_tasks as imvimlita
import helpers.hsql as hsql

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

hprint.config_notebook()


# %% [markdown]
# ## Functions

# %%
def convert_to_the_format_for_analysis(df, suffix):
    """
    This function does the following:
    - Add a column `diff_in_timestamps` which is a time difference from the timestamp in the previous row.
    - Drop the columns that are not necessary for the analysis.
    - Filter the data, so all data starts from the same time.
    - Choose the rows that where the step from the previous timestamp is greater than 1 minute.
    - Add suffix to distiguish between vendors.
    """
    df = df.reset_index()
    df = df.dropna()
    df['diff_in_timestamps'] = df.timestamp - df.timestamp.shift(1)
    df = df.set_index("timestamp")
    df = df[["diff_in_timestamps", "volume"]]
    df = df[df.index > "2022-03-17 00:00:00+00:00"]
    df = df[df["diff_in_timestamps"]!="0 days 00:01:00"]
    df = df.add_suffix(f"{suffix}")
    return df


# %% [markdown]
# # Load the data

# %%
# Specify the connection.
env_file = imvimlita.get_db_env_path("dev")
connection_params = hsql.get_connection_info_from_env_file(env_file)
connection = hsql.get_connection(*connection_params)
# Specify param for both clients.
resample_1min = True

# %%
# General params for `read_data`.
full_symbol = ["binance::ADA_USDT"]
start_date = end_date = None

# %% [markdown]
# ## Load CCXT data

# %% run_control={"marked": false}
# Initiate the client.
vendor = "CCXT"
ccxt_client = imvcdccccl.CcxtCddDbClient(vendor, resample_1min, connection)

# %%
# Load the data.
ada_ccxt = ccxt_client.read_data(full_symbol, start_date, end_date)
display(ada_ccxt.shape)
display(ada_ccxt.head(3))

# %% [markdown]
# ## Load Realtime Talos data

# %% run_control={"marked": false}
# Initialize the client.
table_name = "talos_ohlcv"
mode = "market_data"
talos_client = imvtdctacl.RealTimeSqlTalosClient(resample_1min, connection, table_name, mode)

# %%
# Load the data.
ada_talos = talos_client.read_data(full_symbol, start_date, end_date)
display(ada_talos.shape)
display(ada_talos.head(3))

# %% [markdown]
# # Research of NaNs in timestamps 

# %%
diff_ccxt = convert_to_the_format_for_analysis(ada_ccxt, "_ccxt")
diff_talos = convert_to_the_format_for_analysis(ada_talos, "_talos")

# %%
df = pd.concat([diff_ccxt, diff_talos],axis=1)

# %%
df

# %%
df[(df.diff_in_timestamps_ccxt.isna())|df.diff_in_timestamps_talos.isna()]

# %%
df[df.diff_in_timestamps_ccxt.isna()]

# %%
df[df.diff_in_timestamps_talos.isna()]

# %%
df[(df.diff_in_timestamps_ccxt.notna())&df.diff_in_timestamps_talos.notna()]

# %%
gg = df[(df.diff_in_timestamps_ccxt.notna())&df.diff_in_timestamps_talos.notna()]
gg.volume_ccxt == gg.volume_talos

# %%
