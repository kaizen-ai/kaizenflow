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
# - DB data = CCXT realtime bid-ask data collection for futures
# - CC data = CC bid ask futures data

# %% [markdown]
# # Imports

# %%
import logging
import os

import pandas as pd

import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hprint as hprint
import helpers.hs3 as hs3
import helpers.hsql as hsql
import im_v2.ccxt.data.client as icdcl
import im_v2.crypto_chassis.data.client.crypto_chassis_clients as imvccdcccc
import im_v2.im_lib_tasks as imvimlita

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()


# %% [markdown]
# # Functions

# %%
def load_and_transform_the_data(
    universe, start_ts, end_ts, columns, filter_data_mode, cols, is_db: bool
):
    if is_db:
        df = cc_parquet_client.read_data(
            universe, start_ts, end_ts, columns, filter_data_mode
        )
    else:
        df = db_im_client.read_data(
            universe, start_ts, end_ts, columns, filter_data_mode
        )
        df.index = df.reset_index()["timestamp"].apply(
            lambda x: x.round(freq="T")
        )
    df = df[cols]
    df = df.reset_index().set_index(["timestamp", "full_symbol"])
    return df


# %% [markdown]
# # Initiate clients

# %% run_control={"marked": false}
# DB connection.
env_file = imvimlita.get_db_env_path("dev")
connection_params = hsql.get_connection_info_from_env_file(env_file)
db_connection = hsql.get_connection(*connection_params)
# DB client.
resample_1min = False
db_im_client = icdcl.CcxtSqlRealTimeImClient(
    resample_1min, db_connection, "ccxt_bid_ask_futures_test"
)

# %%
# CC client.
resample_1min = True
universe_version = None
aws_profile = "ck"
s3_bucket_path = hs3.get_s3_bucket_path(aws_profile)
root_dir = os.path.join(s3_bucket_path, "reorg", "daily_staged.airflow.pq")
partition_mode = "by_year_month"
dataset = "bid_ask"
contract_type = "futures"
data_snapshot = ""
cc_parquet_client = imvccdcccc.CryptoChassisHistoricalPqByTileClient(
    universe_version,
    resample_1min,
    root_dir,
    partition_mode,
    dataset,
    contract_type,
    data_snapshot,
    aws_profile=aws_profile,
)

# %% [markdown]
# # Specify universe

# %% run_control={"marked": false}
# DB universe
db_universe = db_im_client.get_universe()
print("DB universe:", "\n", db_universe)
# CC universe.
cc_universe = cc_parquet_client.get_universe()
print("\n", "CC universe:", "\n", cc_universe)
# Intersection of the universes above.
universe = [value for value in db_universe if value in cc_universe]
print("\n", "Intersected universe:", "\n", universe)

# %% [markdown]
# # Load the data

# %% [markdown]
# ## Adjust universe

# %%
# Even though they're in the intersected universe,
# they are not downloaded in CC.
universe.remove("binance::XRP_USDT")
universe.remove("binance::DOT_USDT")

universe

# %% [markdown]
# ## Specify params

# %%
# DB data starts from here.
start_ts = pd.Timestamp("2022-09-08 22:06:00+00:00")
end_ts = columns = None
filter_data_mode = "assert"
bid_ask_cols = ["bid_price", "bid_size", "ask_price", "ask_size", "full_symbol"]

# %% [markdown]
# ## Load data

# %%
data_cc = load_and_transform_the_data(
    universe,
    start_ts,
    end_ts,
    columns,
    filter_data_mode,
    bid_ask_cols,
    is_db=False,
)
data_db = load_and_transform_the_data(
    universe,
    start_ts,
    end_ts,
    columns,
    filter_data_mode,
    bid_ask_cols,
    is_db=True,
)

# %% [markdown]
# # Analysis

# %% [markdown]
# ## Merge CC and DB data into one DataFrame

# %%
data = data_db.merge(
    data_cc, left_index=True, right_index=True, suffixes=("_db", "_cc")
)
print("Start date:", data.reset_index()["timestamp"].min())
print("End date:", data.reset_index()["timestamp"].max())
print(
    "Avg observations per coin:",
    len(data) / len(data.reset_index()["full_symbol"].unique()),
)
display(data.tail())

# %% [markdown]
# ## Calculate differences

# %%
# Full symbol will not be relevant in calculation loops below.
bid_ask_cols.remove("full_symbol")
# Each bid ask value will have a notional and a relative difference between two sources. 
for col in bid_ask_cols:
    # Notional difference: CC value - DB value.
    data[f"{col}_diff"] = data[f"{col}_cc"] - data[f"{col}_db"]
    # Relative value: (CC value - DB value)/DB value.
    data[f"{col}_relative_diff"] = (data[f"{col}_cc"] - data[f"{col}_db"]) / data[
        f"{col}_db"
    ]

data.head()

# %%
# Calculate the mean value of differences for each coin.
diff_stats = []
grouper = data.groupby(["full_symbol"])
for col in bid_ask_cols:
    diff_stats.append(grouper[f"{col}_diff"].mean())
    diff_stats.append(grouper[f"{col}_relative_diff"].mean())

diff_stats = pd.concat(diff_stats, axis=1)

# %% [markdown]
# ## Show stats for differences

# %% [markdown]
# ### Prices

# %%
diff_stats[["bid_price_relative_diff", "ask_price_relative_diff"]]

# %% [markdown]
# As one can see, the difference between bid and ask prices in DB and CC are less than 1%.

# %% [markdown]
# ### Sizes

# %%
diff_stats[["bid_size_relative_diff", "ask_size_relative_diff"]]

# %% [markdown]
# The difference between bid and ask sizes in DB and CC is solid and accounts for more than 100% for each full symbol.

# %% [markdown]
# ## Correlations

# %% [markdown]
# ### Bid price

# %%
bid_price_corr_matrix = data[["bid_price_cc", "bid_price_db"]].groupby(level=1).corr()
bid_price_corr_matrix

# %% [markdown]
# Correlation stats confirms the stats above: bid prices in DB and CC are highly correlated.

# %% [markdown]
# ### Ask price

# %%
ask_price_corr_matrix = data[["ask_price_cc", "ask_price_db"]].groupby(level=1).corr()
ask_price_corr_matrix

# %% [markdown]
# Correlation stats confirms the stats above: ask prices in DB and CC are highly correlated.

# %% [markdown]
# ### Bid size

# %%
bid_size_corr_matrix = data[["bid_size_cc", "bid_size_db"]].groupby(level=1).corr()
bid_size_corr_matrix

# %% [markdown]
# Correlation stats confirms the stats above: bid sizes in DB and CC are not correlated.

# %% [markdown]
# ### Ask size

# %%
ask_size_corr_matrix = data[["ask_size_cc", "ask_size_db"]].groupby(level=1).corr()
ask_size_corr_matrix

# %% [markdown]
# Correlation stats confirms the stats above: ask sizes in DB and CC are not correlated.
