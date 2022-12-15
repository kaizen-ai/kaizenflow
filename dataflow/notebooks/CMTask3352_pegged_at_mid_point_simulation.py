# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
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
import im_v2.ccxt.data.client as icdcl
import im_v2.common.db.db_utils as imvcddbut
import market_data as mdata

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %%
db_manager = imvcddbut.DbConnectionManager()
db_manager.get_connection("dev")
resample_1min = False
table_name = "ccxt_bid_ask_futures_raw"
connection = db_manager.connection
client = icdcl.CcxtSqlRealTimeImClient(resample_1min, connection, table_name)

# %%
# Set the time boundaries.
start_ts = pd.Timestamp("2022-12-14 11:00:00+00:00")
end_ts = pd.Timestamp("2022-12-14 12:00:00+00:00")
intervals = [
    (
        start_ts,
        end_ts,
    ),
]

# %%
universe_str = "ccxt_v7_3-all"
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

# %%
market_data, wall_clock_time = mdata.get_RealTimeImClientMarketData_example1(
    client, asset_ids
)


# %% [markdown]
# ## Filtering data

# %% [markdown]
# - Keep level 1 of the orderbook
# - Check raw data for anomalies
# - Convert to multiindex
# - Add extra columns
# - Resample

# %%
def _run_dag_node(dag):
    dag_runner = dtfcore.FitPredictDagRunner(dag)
    dag_runner.set_fit_intervals(intervals)
    fit_result_bundle = dag_runner.fit()
    df = fit_result_bundle.result_df
    return df


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

# %%
data = _run_dag_node(dag)
data.shape
data = data[
    [
        "bid_size_l1",
        "bid_price_l1",
        "ask_size_l1",
        "ask_price_l1",
        "knowledge_timestamp",
        "full_symbol",
        "start_timestamp",
    ]
]

# %%
data.head(10)

# %% [markdown]
# ### Calculate average difference between datetime rows in the index

# %%
time_diff = data.index.to_series().diff()
time_diff.mean()

# %% [markdown]
# #### Comment

# %% [markdown]
# - The data stream for each asset is downloaded in sequence, and the difference between those averages for about 0.014 seconds.
# - In order to estimate the number of NaNs in the dataframe, we need to drop assets for which this datetime index does not exist.
# - Checking rows for which all relevant data is missing

# %%
original_shape = data.shape[0]
data = data.dropna(
    axis=0,
    subset=data[
        ["bid_size_l1", "ask_size_l1", "bid_price_l1", "ask_price_l1"]
    ].columns,
    how="all",
)
nonna_shape = data.shape[0]
shape_dff = original_shape - nonna_shape

print(
    f"Missing bid/ask data for the given time period: {shape_diff} ({shape_diff / original_shape})"
)

# %%
stage = "resample"
nid = stage
dict_ = {
    "in_col_groups": [
        ("bid_size_l1",),
        ("ask_size_l1",),
        ("bid_price_l1",),
        ("ask_price_l1",),
    ],
    "out_col_group": (),
    "transformer_kwargs": {
        "rule": "1T",
        "resampling_groups": [
            ({"bid_size_l1": "bid_size"}, "sum", {}),
            ({"ask_size_l1": "ask_size"}, "sum", {}),
            ({"bid_price_l1": "bid_price"}, "last", {}),
            ({"ask_price_l1": "ask_price"}, "last", {}),
        ],
    },
    "reindex_like_input": False,
    "join_output_with_input": False,
}
node = dtfcore.GroupedColDfToDfTransformer(
    nid, transformer_func=cofinanc.resample_bars, **dict_
)
dag.append_to_tail(node)
dtfcore.draw(dag)

# %%
