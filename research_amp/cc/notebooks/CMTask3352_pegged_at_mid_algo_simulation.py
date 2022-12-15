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
df = _run_dag_node(dag)
df.shape
df.head(3)

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
df.isna().sum()

# %%
# Check for zeroes.
(df == 0).astype(int).sum(axis=1).sum()

# %% run_control={"marked": false}
# Check bid price !< ask price.
(df["bid_price"] >= df["ask_price"]).any().any()

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
df = cofinanc.process_bid_ask(
    df,
    bid_col,
    ask_col,
    bid_volume_col,
    ask_volume_col,
    requested_cols=requested_cols,
    join_output_with_input=join_output_with_input,
)
df.head(3)

# %% run_control={"marked": false}
# Add limit prices based on passivity of 0.01.
mid_price = df["mid"]
passivity_factor = 0.01

limit_buy_price = df["mid"].resample("1T").mean().shift(1) * (
    1 - passivity_factor
)
limit_sell_price = df["mid"].resample("1T").mean().shift(1) * (
    1 + passivity_factor
)
df[("limit_buy_price", 3303714233)] = limit_buy_price
df[("limit_sell_price", 3303714233)] = limit_sell_price

# %% run_control={"marked": true}
# Count is_buy / is_sell.
df[("is_buy", 3303714233)] = (
    df[("bid_price", 3303714233)] <= df[("limit_buy_price", 3303714233)].ffill()
)
df[("is_sell", 3303714233)] = (
    df[("ask_price", 3303714233)] >= df[("limit_sell_price", 3303714233)].ffill()
)

# %%
# Display as percentages.

display("Successful_buys:",df.drop_duplicates(
    subset=[("bid_price", 3303714233), ("is_buy", 3303714233)], keep="first"
)["is_buy"].value_counts(normalize=True))

# %%
display("Succesful sells:", df.drop_duplicates(
    subset=[("ask_price", 3303714233), ("is_sell", 3303714233)], keep="first"
)["is_sell"].value_counts(normalize=True))

# %% [markdown]
# ### Commentary

# %% [markdown]
# The quick look into the rate of successful trades indicated that for the given asset (`ADA/USDT`) and the date the successful "buy" order can be met for 16% of the time and a "sell" order is not met at all.
