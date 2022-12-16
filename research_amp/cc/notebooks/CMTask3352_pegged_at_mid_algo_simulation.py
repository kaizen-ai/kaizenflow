# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.14.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Description

# %% [markdown]
# The notebook simulates the performance of a "pegged-at-mid" trading algorithm.
#
# - Load the 1sec bid/ask data
# - Conduct a sanity check
# - Calculate the midpoint limit buy/sell prices
# - Aggregate to 5T and check the success of trades in historical data
#
# ```
# dataset_signature={periodic}.{airflow}.{downloaded_EOD}.{parquet}.{bid_ask}.{futures}.{v3}.{cryptochassis}.{binance}.{v1_0_0]}
# ```

# %%
# %load_ext autoreload
# %autoreload 2
import logging

import pandas as pd

import core.finance as cofinanc
import dataflow.core as dtfcore
import dataflow.system as dtfsys
import dataflow.universe as dtfuniver
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hprint as hprint
import im_v2.crypto_chassis.data.client as iccdc
import market_data as mdata

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %% [markdown]
# # Load CryptoChassis data.

# %% [markdown]
# - Latest universe (v3)
# - Resampled to 1sec
# - For 1 asset and 1 day
# - Using DataFlow `read_data` node

# %%
universe_version = "v3"
resample_1min = False
contract_type = "futures"
tag = "downloaded_1sec"
client = iccdc.get_CryptoChassisHistoricalPqByTileClient_example2(
    universe_version, resample_1min, contract_type, tag
)

# %%
# Set the time boundaries.
start_ts = pd.Timestamp("2022-12-14 00:00:00+00:00")
end_ts = pd.Timestamp("2022-12-15 00:00:00+00:00")
intervals = [
    (
        start_ts,
        end_ts,
    ),
]

# %%
universe_str = "crypto_chassis_v3-top1"
full_symbols = dtfuniver.get_universe(universe_str)
asset_ids = client.get_asset_ids_from_full_symbols(full_symbols)

# %%
columns = None
columns_remap = None
wall_clock_time = pd.Timestamp("2100-01-01T00:00:00+00:00")
market_data = mdata.get_HistoricalImClientMarketData_example1(
    client, asset_ids, columns, columns_remap, wall_clock_time=wall_clock_time
)
stage = "read_data"
ts_col_name = "end_ts"
multiindex_output = True
col_names_to_remove = []
market_data = mdata.get_HistoricalImClientMarketData_example1(
    client,
    asset_ids,
    columns,
    columns_remap,
)


# %% [markdown]
# ## Initialize DAG

# %%
def _run_dag_node(dag):
    dag_runner = dtfcore.FitPredictDagRunner(dag)
    dag_runner.set_fit_intervals(intervals)
    fit_result_bundle = dag_runner.fit()
    df = fit_result_bundle.result_df
    return df


# %%
# Create an empty DAG.
dag = dtfcore.DAG(mode="strict")
dtfcore.draw(dag)

# %%
# TODO(gp): @danya, see if we have also close or trade.
stage = "read_data"
ts_col_name = "end_ts"
multiindex_output = True
col_names_to_remove = []
node = dtfsys.HistoricalDataSource(
    stage,
    market_data,
    ts_col_name,
    multiindex_output,
    col_names_to_remove=col_names_to_remove,
)
dag.insert_at_head(node)
dtfcore.draw(dag)

# %% [markdown]
# ## Read data

# %%
df1 = _run_dag_node(dag)
df1.shape
df1.head(3)

# %% [markdown]
# ## Sanity check

# %% [markdown]
# A quick sanity-check for the following:
# - What percentage of 1 sec bars are missing?
# - How often is bid_size = 0, ask_size = 0, volume=0?
# - How often is bid !< ask?
#

# %%
# Check for missing data.
df1.isna().sum()

# %%
# Check for zeroes.
(df1 == 0).astype(int).sum(axis=1).sum()

# %% run_control={"marked": false}
# Check bid price !< ask price.
(df1["bid_price"] >= df1["ask_price"]).any().any()

# %%
df1.head()

# %%
# TODO(gp): There are some missing data, e.g., 19:00:02. Let's compute some quick stats.

# %% [markdown]
# ### Commentary

# %% [markdown]
# Since no NaNs or zeroes were found with a simple general check, there is no need for an in-depth look.

# %% [markdown]
# ## Augment data with new features

# %%
# Append `mid` data.
# # (bid + ask) / 2.
bid_col = "bid_price"
ask_col = "ask_price"
bid_volume_col = "bid_size"
ask_volume_col = "ask_size"
requested_cols = ["mid"]
join_output_with_input = True
df2 = cofinanc.process_bid_ask(
    df1,
    bid_col,
    ask_col,
    bid_volume_col,
    ask_volume_col,
    requested_cols=requested_cols,
    join_output_with_input=join_output_with_input,
)
df2.head(10)

# %%
# TODO(gp): Let's assign dfs to different vars so that each cell is idempotent.
print(df2.shape)
print(df2.index.min())
print(df2.index.max())

# %%
asset_id = 3303714233

# %%
df3 = df2.swaplevel(axis=1)[asset_id]
df3.head()

# %%
df3["ask_value"] = df3["ask_price"] * df3["ask_size"]
df3["bid_value"] = df3["bid_price"] * df3["bid_size"]

# This is really high. 100m USD per hour on top of the book.
df3[["bid_value", "ask_value"]].resample("1H").sum().plot()

# %% run_control={"marked": false}
print(df3.shape)
# Add limit prices based on passivity of 0.01.
mid_price = df3["mid"]
passivity_factor = 0.01

df_limit_price = pd.DataFrame()
# df_limit_price["limit_buy_price"] = df["mid"].resample("1T").mean().shift(1) * (
#     1 - passivity_factor
# )
# df_limit_price["limit_sell_price"] = df["mid"].resample("1T").mean().shift(1) * (
#     1 + passivity_factor
# )
    
# TODO(gp): This should be tuned as function of the rolling std dev.
abs_spread = 0.0001
# We are trading at the top of the book.
# TODO(gp): Crossing the spread means setting limit_buy_price = ... + abs_spread (and vice versa for sell).
df_limit_price["limit_buy_price"] = df3["mid"].resample("1T").mean().shift(1) - abs_spread
df_limit_price["limit_sell_price"] = df3["mid"].resample("1T").mean().shift(1) + abs_spread
    
df4 = df3.merge(df_limit_price, right_index=True, left_index=True, how="outer")
df4["limit_buy_price"] = df4["limit_buy_price"].ffill()
df4["limit_sell_price"] = df4["limit_sell_price"].ffill()
print(df4.shape)

# %%
# TODO(gp): Not sure this is working as expected. I don't see the seconds.
# Assigning df columns with a df series with different time index might subsample.
# I would do a outmerge merge and then ffill.

# Let's always check the output of the df to make sure things are sane.
df4

# %%
spread = df4["ask_price"] - df4["bid_price"]
spread_in_bps = spread / df4["mid"] * 1e4

# %%
spread.hist(bins=101)

# %%
spread.plot()

# %%
spread_in_bps.plot()

# %%
#df4[["mid", "ask_price", "bid_price"]].head(200).plot()
#df4[["mid", "ask_price", "bid_price", "limit_buy_price", "limit_sell_price"]].head(10000).plot()
df4[["mid", "ask_price", "bid_price", "limit_buy_price", "limit_sell_price"]].head(1000).plot()

# %% run_control={"marked": true}
# Count is_buy / is_sell.
df4["is_buy"] = (
    df4["ask_price"] <= df4["limit_buy_price"]
)
df4["is_sell"] = (
    df4["bid_price"] >= df4["limit_sell_price"]
)

(df4[["is_buy", "is_sell"]] * 1.0).head(1000).plot()

# %%
print(df4.shape)

# %%
# Display as percentages.

# display("Successful_buys:",df.drop_duplicates(
#     subset=["bid_price", "is_buy", 3303714233)], keep="first"
# )["is_buy"].value_counts(normalize=True))

print(df4["is_buy"].sum() / df4.shape[0])
print(df4["is_sell"].sum() / df4.shape[0])

# %%
import helpers.hprint as hprint

# %%
import numpy as np

# %%
mask.sum() / mask.shape[0]

# %%
# TODO(gp): ask_price -> buy_limit?
df4["exec_buy_price"] = df4["is_buy"] * df4["ask_price"]
mask = ~df4["is_buy"]
df4["exec_buy_price"][mask] = np.nan

#df4["exec_price"].plot()
#df4["exec_price"].mean()
print(hprint.perc(df4["exec_buy_price"].isnull().sum(), df4.shape[0]))

# TODO(gp): Not sure this does what we want. We want to average only the values that are not nan.
#df4["exec_buy_price"].resample("5T").mean()

df4["exec_sell_price"] = df4["is_sell"] * df4["bid_price"]

mask = ~df4["is_sell"]
df4["exec_sell_price"][mask] = np.nan

#df4["exec_price"].plot()
#df4["exec_price"].mean()
print(hprint.perc(df4["exec_sell_price"].isnull().sum(), df4.shape[0]))

# %%
df5 = pd.DataFrame()
# Count how many executions there were in an interval.
df5["exec_buy_num"] = df4["is_buy"].resample("5T").sum()
df5["exec_buy_price"] = df4["exec_buy_price"].resample("5T").mean()
df5["exec_is_buy"] = df5["exec_buy_num"] > 0
print(hprint.perc(df5["exec_is_buy"].sum(), df5["exec_is_buy"].shape[0]))

# Estimate the executed volume. 
df5["exec_buy_volume"] = (df4["ask_size"] * df4["ask_price"] * df4["is_buy"]).resample("5T").sum()
print("million USD per 5T=", df5["exec_buy_volume"].mean() / 1e6)
# Estimate price as average of executed price.
#exec_buy_price = (close * exec_is_buy).groupby("5T").sum() / exec_buy_num

# Same for sell.
#exec_sell_volume = (is_sell * bid_size).group("5T").sum()
#exec_sell_price = (close * exec_is_sell).groupby("5T").sum() / exec_sell_num

df5["exec_sell_num"] = df4["is_sell"].resample("5T").sum()
df5["exec_sell_price"] = df4["exec_sell_price"].resample("5T").mean()
df5["exec_is_sell"] = df5["exec_sell_num"] > 0
print(hprint.perc(df5["exec_is_sell"].sum(), df5["exec_is_sell"].shape[0]))

# Estimate the executed volume. 
df5["exec_sell_volume"] = (df4["bid_size"] * df4["bid_price"] * df4["is_sell"]).resample("5T").sum()
print("million USD per 5T=", df5["exec_sell_volume"].mean() / 1e6)

# %%
# This is the benchmark.
df5["twap_mid_price"] = df4["mid"].resample("5T").mean()

df5[["twap_mid_price", "exec_sell_price", "exec_buy_price"]].head(1000).plot()

# %%
slippage = df5[["twap_mid_price", "exec_sell_price", "exec_buy_price"]]

slippage["sell_slippage_bps"] = (df5["exec_sell_price"] - df5["twap_mid_price"]) / df5["twap_mid_price"] * 1e4

#slippage = df["twap_mid_price"] / 

slippage["sell_slippage_bps"].hist(bins=21)

print("sell_slippage_bps.mean=", slippage["sell_slippage_bps"].mean())
print("sell_slippage_bps.median=", slippage["sell_slippage_bps"].median())

# %%
df5.head()

# %% [markdown]
# ### Commentary

# %% [markdown]
# The quick look into the rate of successful trades indicated that for the given asset (`ADA/USDT`) and the date the successful "buy" order can be met for 16% of the time and a "sell" order is not met at all.
