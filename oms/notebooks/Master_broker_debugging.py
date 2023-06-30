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

# %% [markdown]
# # Description

# %% [markdown]
# This notebook contains tools for debugging an individual `CcxtBroker` execution.
#
# The notebook is responsible for aspects of the execution experiment run that are related to the correctness of the execution, as opposed to the analysis of the result:
# - Consistency of logging
# - Correctness in the submission of orders
# - Consistency between trades and submitted orders
# - Order time profiling

# %% run_control={"marked": false}
# %load_ext autoreload
# %autoreload 2

import logging

import pandas as pd

import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hprint as hprint
import oms.ccxt.ccxt_filled_orders_reader as occforre

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %%
# Here provide a directory containing experiment results.
log_dir = "/app/system_log_dir/"
ccxt_log_reader = occforre.CcxtLogsReader(log_dir)

# %%
data = ccxt_log_reader.load_all_data()
ccxt_order_response_df = data["ccxt_order_responses"]
ccxt_trades_df = data["ccxt_trades"]
child_order_df = data["oms_child_orders"]

# %% [markdown]
# ## Child order responses

# %%
ccxt_order_response_df.info()

# %% [markdown]
# ## Child orders

# %%
child_order_df.info()

# %%
# Convert ccxt_id to string to avoid type confusion.
child_order_df["ccxt_id"] = child_order_df["ccxt_id"].astype(str)

# %%
child_order_df["extra_params"][0]

# %% [markdown]
# ## Trades

# %%
ccxt_trades_df.info()

# %% [markdown]
# ### Verify that all trades correspond to logged child orders

# %%
# Verify that all trades have an associated order response.
# If the trade does not have an associated order, it means
# that a trade was executed that was not accounted for during the experiment.
ccxt_trades_df["order"].isin(ccxt_order_response_df["order"])

# %%
# Verify that all trades have an associated 'ccxt_id' in OMS child orders.
# This verifies that there is a consistency between a CCXT order, OMS order and
# a CCXT trade. If a trade has no associated OMS child order `ccxt_id`, it means
# that an unexpected trade was executed, for example, by a different actor on the same account.
ccxt_trades_df["order"].isin(child_order_df["ccxt_id"])

# %% [markdown]
# ### Check orders that do not correspond to any trades

# %% run_control={"marked": true}
# Existence of such orders is not necessarily a bug.
# It means that a given OMS child order was not filled.
child_orders_with_no_trades = ~child_order_df["ccxt_id"].isin(
    ccxt_trades_df["order"]
)
child_orders_with_no_trades.sum()

# %%
child_order_df.loc[child_orders_with_no_trades]

# %% [markdown]
# ### Check the correctness of trade amount
# - Sum of `amount` of all trades related to a single child order should not exceed the total amount for that order.
#

# %% run_control={"marked": false}
# If the traded amount is larger than the order, it means that an extra trade
# is executed and the order is filled for a larger amount.
# This can mean an accounting error on the exchange side.
trade_amount_by_order = ccxt_trades_df.groupby("order").agg({"amount": sum})
trade_amount_by_order["child_order_amount"] = child_order_df.set_index("ccxt_id")[
    "diff_num_shares"
].abs()

# %%
mask = (
    trade_amount_by_order["amount"] > trade_amount_by_order["child_order_amount"]
)
trade_amount_by_order.loc[mask]

# %% [markdown]
# ### Verify that the number of child order responses equals number of child orders

# %%
# If the check is not passed, it means that some submitted child orders
# never reached the exchange and were not posted.
len(child_order_df) == len(ccxt_order_response_df)

# %% [markdown]
# ### Verify that CCXT IDs are equal in both child orders and responses

# %%
# Verify that we are not logging any CCXT orders that are not a part
# of the experiment, i.e. have no OMS order associated with it.
set(child_order_df["ccxt_id"].unique()) == set(
    ccxt_order_response_df["order"].unique()
)

# %% [markdown]
# # Time profiling for child orders

# %%
# TODO(Toma): Update timestamp logging to use datetime.
child_order_df_with_timestamps = ccxt_log_reader.load_oms_child_order_df(
    unpack_extra_params=True
)

# %%
child_order_timestamp_stats = child_order_df_with_timestamps["stats"].apply(
    pd.Series
)

# %%
child_order_timestamp_stats
