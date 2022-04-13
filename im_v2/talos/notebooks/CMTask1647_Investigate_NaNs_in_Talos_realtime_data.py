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
import logging

import pandas as pd

import helpers.hdbg as hdbg
import helpers.hprint as hprint
import helpers.hsql as hsql
import im_v2.ccxt.data.client.ccxt_clients as imvcdccccl
import im_v2.im_lib_tasks as imvimlita
import im_v2.talos.data.client.talos_clients as imvtdctacl

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
    df["diff_in_timestamps"] = df.timestamp - df.timestamp.shift(1)
    df = df.set_index("timestamp")
    df = df[["diff_in_timestamps"]]
    df = df[df.index > "2022-03-17 00:00:00+00:00"]
    df = df[df["diff_in_timestamps"] != "0 days 00:01:00"]
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
talos_client = imvtdctacl.RealTimeSqlTalosClient(
    resample_1min, connection, table_name, mode
)

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
# The unique DataFrame with the comparison of NaN data.
df = pd.concat([diff_ccxt, diff_talos], axis=1)
# Add a column that shows the difference between NaN sequences of vendors.
df["diff"] = df["diff_in_timestamps_talos"] - df["diff_in_timestamps_ccxt"]
df.head(3)

# %% [markdown]
# The description of the columns in the created DataFrame:
# - `timestamp` - Shows the first piece of data that appears after NaN sequence.
# - `diff_in_timestamps_ccxt` - Shows the time value of sequence of NaNs in CCXT data.
# - `diff_in_timestamps_talos` - Same as above but for Talos.
# - `diff` - Difference between NaN sequences of vendors.

# %%
# Cases where both vendors have NaN sequences.
df[(df.diff_in_timestamps_ccxt.notna()) & df.diff_in_timestamps_talos.notna()]

# %% [markdown]
# An important notice is that the most NaN sequences are ending at the same time in both vendors that is an indicator of this data is absent on the data provider side.

# %%
# The data is presented in CCXT, but not in Talos.
df[df.diff_in_timestamps_ccxt.isna()]

# %%
# The data is presented in Talos, but not in CCXT.
df[df.diff_in_timestamps_talos.isna()]

# %%
num_both_seq = df[
    (df.diff_in_timestamps_ccxt.notna()) & df.diff_in_timestamps_talos.notna()
].shape[0]
num_unique_seq_ccxt = df[df.diff_in_timestamps_talos.isna()].shape[0]
num_unique_seq_talos = df[df.diff_in_timestamps_ccxt.isna()].shape[0]

total_time_talos = df["diff_in_timestamps_talos"].sum()
total_time_ccxt = df["diff_in_timestamps_ccxt"].sum()
diff_in_total_time = total_time_talos - total_time_ccxt
mean_time_diff = df["diff"].mean()

print(
    f"Number of NaN sequences that are the same in both vendors: {num_both_seq}"
)
print(
    f"Number of NaN sequences that are presented in CCXT, but not in Talos: {num_unique_seq_ccxt}"
)
print(
    f"Number of NaN sequences that are presented in Talos, but not in CCXT: {num_unique_seq_talos}"
)

print(f"Total time of NaN sequences in Talos - {total_time_talos}")
print(f"Total time of NaN sequences in CCXT - {total_time_ccxt}")
print(
    f"Talos NaN sequences are greater than CCXT by the amount of {diff_in_total_time}"
)
print(
    f"Mean difference of NaN sequence between two vendors (Talos has greater sequences) - {mean_time_diff}"
)

# %%
