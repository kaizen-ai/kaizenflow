# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.15.0
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

import core.config as cconfig
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hprint as hprint
import oms.broker.ccxt.ccxt_aggregation_functions as obccagfu
import oms.broker.ccxt.ccxt_logs_reader as obcclore

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %%
config = cconfig.get_config_from_env()
if config:
    # Get config from env when running the notebook via the `run_notebook.py` script, e.g.,
    # in the system reconciliation flow.
    _LOG.info("Using config from env vars")
else:
    system_log_dir = "/shared_data/ecs/test/twap_experiment/20230814_1"
    config_dict = {"system_log_dir": system_log_dir}
    config = cconfig.Config.from_dict(config_dict)
print(config)

# %% run_control={"marked": false}
log_dir = config["system_log_dir"]
ccxt_log_reader = obcclore.CcxtLogsReader(log_dir)
#
data = ccxt_log_reader.load_all_data()
ccxt_order_response_df = data["ccxt_order_responses"]
ccxt_trades_df = data["ccxt_trades"]
oms_child_order_df = data["oms_child_orders"]
oms_parent_order_df = data["oms_parent_orders"]

# %%
ccxt_order_response_df.head(3)

# %%
oms_parent_order_df.iloc[0]["extra_params"]

# %% [markdown]
# ## Child order responses

# %%
ccxt_order_response_df.info()

# %% [markdown]
# ## Child orders

# %%
# If the value of total child orders is lower than expected,
oms_child_order_df.info()

# %%
oms_child_order_df.head(3)

# %% [markdown]
# ### Verify that OMS child orders have maximum of one CCXT order ID

# %% [markdown]
# Originally the CCXT IDs are loaded as `List[str]`. Each child order should correspond to 1 CCXT ID (`-1` in case the order was not submitted).
#
# Exceptions to this rule indicate a serious error in accounting.

# %%
all(oms_child_order_df["ccxt_id"].apply(lambda x: len(x)) == 1)

# %%
# Unpack single CCXT IDs from lists.
oms_child_order_df["ccxt_id_as_single_str"] = oms_child_order_df["ccxt_id"].apply(
    lambda x: str(x[0])
)

# %% [markdown]
# ### Extract the OMS parent order ID

# %%
oms_child_order_df["parent_order_id"] = oms_child_order_df["extra_params"].apply(
    lambda x: x["oms_parent_order_id"]
)
oms_child_order_df.head(3)

# %%
# Check how many child orders are generated per parent_order_id.
oms_child_order_df["parent_order_id"].value_counts().value_counts()

# %% [markdown]
# ### Check the error messages in non-submitted orders.

# %%
# Select the OMS child orders with no CCXT ID and check their error messages.
not_submitted_oms_child_order_df = oms_child_order_df.loc[
    oms_child_order_df["ccxt_id_as_single_str"] == "-1"
]
print(
    "Number of not submitted OMS child orders=%s out of total orders=%s"
    % (not_submitted_oms_child_order_df.shape[0], oms_child_order_df.shape[0])
)

# %%
# Extract all error messages from unsubmitted orders.
not_submitted_oms_child_order_df["error_msg"] = not_submitted_oms_child_order_df[
    "extra_params"
].apply(lambda x: x["error_msg"])
not_submitted_oms_child_order_df["error_msg"].value_counts()

# %% [markdown]
# ## Parent orders

# %%
oms_parent_order_df.head(3)

# %%
oms_parent_order_df["child_order_ccxt_ids"] = oms_parent_order_df[
    "extra_params"
].apply(lambda x: x["ccxt_id"])
oms_parent_order_df.head(3)

# %%
# Check how many child orders per parent order were successfully submitted.
# A value between maximum expected and 0 can mean that some child orders
# could not get through, while otherwise being well-formed.
# A value of 0 means that the parent order had an issue, e.g.
# having a notional that made all child orders below a minimum.
oms_parent_order_df["submitted_child_order_num"] = oms_parent_order_df[
    "child_order_ccxt_ids"
].str.len()
oms_parent_order_df["submitted_child_order_num"].value_counts()

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
all(ccxt_trades_df["order"].isin(ccxt_order_response_df["order"]))

# %%
# Verify that all trades have an associated 'ccxt_id' in OMS child orders.
# This verifies that there is a consistency between a CCXT order, OMS order and
# a CCXT trade. If a trade has no associated OMS child order `ccxt_id`, it means
# that an unexpected trade was executed, for example, by a different actor on the same account.
all(ccxt_trades_df["order"].isin(oms_child_order_df["ccxt_id_as_single_str"]))

# %% [markdown]
# ### Check orders that do not correspond to any trades

# %% run_control={"marked": true}
# Existence of such orders is not necessarily a bug.
# It means that a given OMS child order was not filled.
child_orders_with_no_trades = ~oms_child_order_df["ccxt_id_as_single_str"].isin(
    ccxt_trades_df["order"]
)
child_orders_with_no_trades.sum()

# %%
oms_child_order_df.loc[child_orders_with_no_trades]

# %% [markdown]
# ### Check the correctness of trade amount
# - Sum of `amount` of all trades related to a single child order should not exceed the total amount for that order.
#

# %% run_control={"marked": false}
# If the traded amount is larger than the order, it means that an extra trade
# is executed and the order is filled for a larger amount.
# This can mean an accounting error on the exchange side.
trade_amount_by_order = ccxt_trades_df.groupby("order").agg({"amount": sum})
accepted_child_order_df = oms_child_order_df.loc[
    oms_child_order_df["ccxt_id_as_single_str"] != "-1"
]
trade_amount_by_order["child_order_amount"] = accepted_child_order_df.set_index(
    "ccxt_id_as_single_str"
)["diff_num_shares"].abs()

# %%
mask = (
    trade_amount_by_order["amount"] > trade_amount_by_order["child_order_amount"]
)
trade_amount_by_order.loc[mask]

# %% [markdown]
# ### Verify that CCXT IDs are equal in both child orders and responses

# %%
# Verify that we are not logging any CCXT orders that are not a part
# of the experiment, i.e. have no OMS order associated with it.
# We expect all retrieved responses to be a subset of CCXT IDs
# connected to the OMS child orders.
submitted_oms_orders_ccxt_ids = set(
    oms_child_order_df.loc[oms_child_order_df["ccxt_id_as_single_str"] != "-1"][
        "ccxt_id_as_single_str"
    ].unique()
)
ccxt_response_ids = set(ccxt_order_response_df["order"].unique())
ccxt_response_ids.issubset(submitted_oms_orders_ccxt_ids)

# %% [markdown]
# # Group trades by order

# %%
# Aggregate fills by order.
# TODO(Danya): Rename to `aggregate_trades_by_order`
ccxt_trades_by_order = obccagfu.aggregate_fills_by_order(ccxt_trades_df)
ccxt_trades_by_order.head(3)

# %% [markdown]
# # Time profiling for child orders

# %%
oms_child_order_df = ccxt_log_reader.load_oms_child_order_df(
    unpack_extra_params=True
)


# %%

def get_oms_child_order_timestamps(df, order_wave: int):
    stats = df["stats"].apply(
        pd.Series
    )
    # Select bid/ask timestamp columns from the DB.
    bid_ask_timestamp_cols = [
        "exchange_timestamp",
        "end_download_timestamp",
        "knowledge_timestamp",
    ]
    bid_ask_timestamps_df = df[bid_ask_timestamp_cols]
    # Combine all timestamp info into single dataframe.
    out_df = pd.concat([bid_ask_timestamps_df, stats], axis=1)
    timing_cols = submission_timestamp_cols = [
        f"_submit_twap_child_order::bid_ask_market_data.start.{order_wave}",
        f"_submit_twap_child_order::bid_ask_market_data.done.{order_wave}",
        f"_submit_twap_child_order::get_open_positions.done.{order_wave}",
        f"_submit_twap_child_order::child_order.created.{order_wave}",
        f"_submit_twap_child_order::child_order.limit_price_calculated.{order_wave}",
        "_submit_single_order_to_ccxt_with_retry::start.timestamp",
        "_submit_single_order_to_ccxt_with_retry::end.timestamp",
        f"_submit_twap_child_order::child_order.submitted.{order_wave}", 
    ]
    timing_cols = bid_ask_timestamp_cols + submission_timestamp_cols
    out_df = out_df[timing_cols].dropna(subset=submission_timestamp_cols)
    return out_df

def get_time_delay_between_events(df):
    """
    For each timestamp, get delay in seconds before the previous timestammp.
    """
    delays = df.subtract(df["exchange_timestamp"], axis=0)
    delays = delays.apply(lambda x: x.dt.total_seconds())
    # Verify that the timestamps increase left to right.
    hdbg.dassert_eq((delays.diff(axis=1) < 0).sum().sum(), 0)
    return delays


# %%
wave = get_oms_child_order_timestamps(oms_child_order_df, 0)

# %%
wave.info()

# %%
time_delays = get_time_delay_between_events(wave)

# %%
time_delays.boxplot(rot=45, ylabel="Time delay")

# %%
