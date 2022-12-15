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
read_data_df = _run_dag_node(dag)
read_data_df.shape
read_data_df.head(3)

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
read_data_df.isna().sum()

# %%
# Check for zeroes.
(read_data_df == 0).astype(int).sum(axis=1).sum()

# %% run_control={"marked": true}
# Check bid price !< ask price.
(read_data_df["bid_price"] >= read_data_df["ask_price"]).any().any()

# %% [markdown]
# ### Commentary

# %% [markdown]
# Since no NaNs or zeroes were found with a simple general check, there is no need for an in-depth look.

# %%
