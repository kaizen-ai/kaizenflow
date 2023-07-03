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
#
# Use bid-ask data to analyze and simulate trading execution quality.

# %%
# %load_ext autoreload
# %autoreload 2

import logging

import numpy as np
import pandas as pd

import core.config as cconfig
import core.finance as cofinanc
import core.plotting as coplotti
import core.statistics as costatis
import dataflow.core as dtfcore
import dataflow.pipelines.execution.execution_pipeline as dtfpexexpi
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hprint as hprint
import im_v2.common.data.client.im_raw_data_client as imvcdcimrdc
import im_v2.common.universe.universe_utils as imvcuunut
import oms
import oms.ccxt_filled_orders_reader as ocfiorre

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %% [markdown]
# # Config

# %%
system_log_dir = "/shared_data/ecs/preprod/twap_experiment/system_reconciliation/C3a/20230419/system_log_dir.scheduled.20230419_041000.20230419_100500"
bar_duration = "5T"
id_col = "asset_id"

# %%
ccxt_log_reader = ocfiorre.CcxtLogsReader(system_log_dir)

# %%
btc_usdt_id = 1467591036

# %%
use_historical = True

# %% [markdown]
# # Load order responses and fills

# %%
ccxt_order_response_df = ccxt_log_reader.load_ccxt_order_response_df()
fills_df = ccxt_log_reader.load_ccxt_fills_df()

# %%
trade_prices = ocfiorre.compute_buy_sell_prices_by_bar(
    fills_df, bar_duration, groupby_id_col=id_col
)

# %%
child_order_df = ccxt_log_reader.load_child_order_df()

# %% [markdown]
# # Load bid-ask data

# %%
# TODO(Paul): Refine the cuts around the first and last bars.
start_timestamp = ccxt_order_response_df["order_response_datetime"].min()
_LOG.info("start_timestamp=%s", start_timestamp)
end_timestamp = fills_df["datetime"].max()
_LOG.info("end_timestamp=%s", end_timestamp)


# %%
def load_bid_ask_data(
    start_timestamp,
    end_timestamp,
    use_historical,
    asset_ids,
) -> pd.DataFrame:
    if use_historical:
        signature = "periodic_daily.airflow.downloaded_1sec.parquet.bid_ask.futures.v3.crypto_chassis.binance.v1_0_0"
        reader = imvcdcimrdc.RawDataReader(signature)
    else:
        signature = "realtime.airflow.downloaded_200ms.postgres.bid_ask.futures.v7.ccxt.binance.v1_0_0"
        reader = imvcdcimrdc.RawDataReader(signature)
    bad = reader.read_data(start_timestamp, end_timestamp)
    hdbg.dassert(not bad.empty, "Requested bid-ask data not available.")
    #
    currency_pair_to_full_symbol = {
        x: "binance::" + x for x in bad["currency_pair"].unique()
    }
    asset_id_to_full_symbol = imvcuunut.build_numerical_to_string_id_mapping(
        currency_pair_to_full_symbol.values()
    )
    full_symbol_mapping_to_asset_id = {
        v: k for k, v in asset_id_to_full_symbol.items()
    }
    currency_pair_to_asset_id = {
        x: full_symbol_mapping_to_asset_id[currency_pair_to_full_symbol[x]]
        for x in bad["currency_pair"].unique()
    }
    # Add asset_ids
    list(currency_pair_to_asset_id.values())
    bad_asset_id = bad["currency_pair"].apply(
        lambda x: currency_pair_to_asset_id[x]
    )
    bad["asset_id"] = bad_asset_id
    #
    if asset_ids is not None:
        bad = bad[bad["asset_id"].isin(asset_ids)]
    if not use_historical:
        bad = bad[
            ["bid_price_l1", "ask_price_l1", "asset_id", "knowledge_timestamp"]
        ].rename(
            columns={"bid_price_l1": "bid_price", "ask_price_l1": "ask_price"},
        )
        bad = bad.pivot_table(columns=["asset_id"], index="knowledge_timestamp")
    else:
        bad = bad[["bid_price_l1", "ask_price_l1", "asset_id"]].rename(
            columns={"bid_price_l1": "bid_price", "ask_price_l1": "ask_price"},
        )
        bad = bad.pivot(columns=["asset_id"])
    bad.index = bad.index.ceil("1s")
    bad = bad.resample("1s").mean().ffill()
    # if use_historical:
    #     bad.index.tz_localize("utc")
    return bad


# %%
bad = load_bid_ask_data(
    start_timestamp,
    end_timestamp,
    use_historical,
    trade_prices.columns.levels[1],
)

# %%
bad.head()

# %% [markdown]
# # Replay limit orders and simulate trades
#
# - Use actual limit prices seen in actual execution
# - Simulate execution using actual limit prices and bid-ask data
# - Compare simulated execution to actual execution

# %% [markdown]
# ## Extract the actual limit orders

# %%
bad.columns.levels[1]

# %%
child_order_df_restricted = child_order_df[
    child_order_df["asset_id"].isin(bad.columns.levels[1])
]

# %%
child_order_df_restricted.head()

# %%
# Forward fill to represent the time-in-force of the underlying order
limit_prices = ocfiorre.get_limit_order_price(child_order_df_restricted)

# limit_prices.index = limit_prices.index.tz_localize("UTC")
limit_prices.head()

# %%
buy_order_num = np.sign(limit_prices["buy_limit_order_price"]).abs().cumsum()
sell_order_num = np.sign(limit_prices["sell_limit_order_price"]).abs().cumsum()

# %%
limit_prices = pd.concat(
    {
        "buy_limit_order_price": limit_prices["buy_limit_order_price"],
        "sell_limit_order_price": limit_prices["sell_limit_order_price"],
        "buy_order_num": buy_order_num,
        "sell_order_num": sell_order_num,
    },
    axis=1,
).ffill(limit=59)

# %% [markdown]
# ## Join limit orders with bid-ask data and simulate trades

# %%
# TODO(Paul): Historical bid/ask data needs to be lagged 1 minute
#  to properly align. May be due to beginning-of-bar vs end-of-bar
# . timestamp conventions.
in_df = pd.concat([limit_prices, bad.shift(60)], axis=1)
in_df.head()

# %%
node = dtfcore.GroupedColDfToDfTransformer(
    "estimate_limit_order_execution",
    transformer_func=cofinanc.estimate_limit_order_execution,
    **{
        "in_col_groups": [
            ("bid_price",),
            ("ask_price",),
            ("buy_limit_order_price",),
            ("sell_limit_order_price",),
            ("buy_order_num",),
            ("sell_order_num",),
        ],
        "out_col_group": (),
        "transformer_kwargs": {
            "bid_col": "bid_price",
            "ask_col": "ask_price",
            "buy_limit_price_col": "buy_limit_order_price",
            "sell_limit_price_col": "sell_limit_order_price",
            "buy_order_num_col": "buy_order_num",
            "sell_order_num_col": "sell_order_num",
        },
    },
)

# %%
simulated_execution_df = node.fit(in_df)["df_out"]

# %%
simulated_execution_df.columns.levels[0].to_list()

# %%
btc_exec = oms.get_asset_slice(simulated_execution_df, btc_usdt_id)

# %%
btc_exec.loc[~btc_exec["buy_trade_price"].isna()][
    [
        "buy_trade_price",
        "buy_limit_order_price",
        "buy_order_num",
        "bid_price",
        "ask_price",
    ]
]

# %%
simulated_execution_df["buy_trade_price"].resample(
    "5T", closed="right", label="right"
).mean()

# %%
active_cols = ["buy_limit_order_price", "buy_trade_price"]

# %%
btc_slice = oms.get_asset_slice(simulated_execution_df, btc_usdt_id)

# %%
btc_slice[active_cols].plot()

# %%
btc_slice["buy_trade_price"].ffill(limit=59).plot()

# %% [markdown]
# ## Compute simulated trade execution quality against bid-ask benchmarks

# %%
simulated_execution_quality_node = dtfcore.GroupedColDfToDfTransformer(
    "simulated_execution_quality",
    transformer_func=cofinanc.compute_execution_quality,
    **{
        "in_col_groups": [
            ("buy_trade_price",),
            ("sell_trade_price",),
            ("bid_price",),
            ("ask_price",),
        ],
        "out_col_group": (),
        "transformer_kwargs": {
            "bid_col": "bid_price",
            "ask_col": "ask_price",
            "buy_trade_price_col": "buy_trade_price",
            "sell_trade_price_col": "sell_trade_price",
        },
    },
)

# %%
simulated_execution_quality_df = simulated_execution_quality_node.fit(
    simulated_execution_df
)["df_out"]

# %%
simulated_execution_quality_df.columns.levels[0].to_list()

# %%
active_cols = ["buy_limit_order_price", "buy_trade_price"]

# %%
btc_slice = oms.get_asset_slice(simulated_execution_quality_df, btc_usdt_id)

# %%
btc_slice[active_cols].dropna(how="all")

# %%
col = "buy_trade_midpoint_slippage_bps"
coplotti.plot_slippage_boxplot(
    simulated_execution_quality_df[col], "by_asset", ylabel=col
)

# %%
simulated_execution_quality_df["buy_trade_midpoint_slippage_bps"].unstack().hist(
    bins=31
)

# %%
col = "sell_trade_midpoint_slippage_bps"
coplotti.plot_slippage_boxplot(
    simulated_execution_quality_df[col], "by_asset", ylabel=col
)

# %%
simulated_execution_quality_df["sell_trade_midpoint_slippage_bps"].unstack().hist(
    bins=31
)

# %%
costatis.compute_moments(
    simulated_execution_quality_df["buy_trade_midpoint_slippage_bps"].unstack()
)

# %%
costatis.compute_moments(simulated_execution_quality_df["spread_bps"].unstack())

# %% [markdown]
# ## Compare actual trade prices to simulated trade prices

# %%
actual_vs_sim_trade_price_resampling_freq = "1T"

# %%
# The "8s" is an empirically-derived estimate of order delay.
simulated_execution_df["sell_limit_order_price"].resample(
    actual_vs_sim_trade_price_resampling_freq,
    closed="right",
    label="right",
    offset="8s",
).mean().head()

# %%
trade_prices = ocfiorre.compute_buy_sell_prices_by_bar(
    fills_df,
    actual_vs_sim_trade_price_resampling_freq,
    offset="8s",
    groupby_id_col="asset_id",
)
trade_prices.head()

# %%
resampled_simulated_execution_df = simulated_execution_df.resample(
    actual_vs_sim_trade_price_resampling_freq,
    closed="right",
    label="right",
    offset="8s",
).mean()
resampled_simulated_execution_df.head()

# %%
actual_trade_prices = ocfiorre.compute_buy_sell_prices_by_bar(
    fills_df, "1s", offset="0s", groupby_id_col="asset_id"
)


# %%
def combine_sim_and_actual_trades(simulated_execution_df, fills, freq, offset):
    #
    actual_trade_prices = ocfiorre.compute_buy_sell_prices_by_bar(
        fills,
        freq,
        offset=offset,
        groupby_id_col="asset_id",
    )
    resampled_simulated_execution_df = simulated_execution_df.resample(
        freq,
        closed="right",
        label="right",
        offset=offset,
    ).mean()
    #
    col_set = actual_trade_prices.columns.levels[1].union(
        resampled_simulated_execution_df.columns.levels[1]
    )
    col_set = col_set.sort_values()
    #
    trade_price_dict = {
        "actual_buy_trade_price": actual_trade_prices["buy_trade_price"].reindex(
            columns=col_set
        ),
        "actual_sell_trade_price": actual_trade_prices[
            "sell_trade_price"
        ].reindex(columns=col_set),
        "simulated_buy_trade_price": resampled_simulated_execution_df[
            "buy_trade_price"
        ].reindex(columns=col_set),
        "simulated_sell_trade_price": resampled_simulated_execution_df[
            "sell_trade_price"
        ].reindex(columns=col_set),
    }
    simulated_and_actual_trade_price_df = pd.concat(trade_price_dict, axis=1)
    return simulated_and_actual_trade_price_df


# %%
actual_vs_sim_trade_price_resampling_freq = "5T"

# %%
simulated_and_actual_trade_price_df = combine_sim_and_actual_trades(
    simulated_execution_df,
    fills_df,
    actual_vs_sim_trade_price_resampling_freq,
    offset="8s",
)
simulated_and_actual_trade_price_df.head()

# %%
oms.get_asset_slice(simulated_and_actual_trade_price_df, btc_usdt_id)[
    ["actual_buy_trade_price", "simulated_buy_trade_price"]
].dropna(how="all")

# %%
oms.get_asset_slice(simulated_and_actual_trade_price_df, btc_usdt_id)[
    ["actual_sell_trade_price", "simulated_sell_trade_price"]
].dropna(how="all")

# %%
execution_quality_node = dtfcore.GroupedColDfToDfTransformer(
    "execution_quality",
    transformer_func=ocfiorre.compute_execution_quality,
    **{
        "in_col_groups": [
            ("actual_buy_trade_price",),
            ("actual_sell_trade_price",),
            ("simulated_buy_trade_price",),
            ("simulated_sell_trade_price",),
        ],
        "out_col_group": (),
        "transformer_kwargs": {
            "buy_trade_reference_price_col": "simulated_buy_trade_price",
            "sell_trade_reference_price_col": "simulated_sell_trade_price",
            "buy_trade_price_col": "actual_buy_trade_price",
            "sell_trade_price_col": "actual_sell_trade_price",
        },
    },
)

# %%
sim_vs_actual_execution_quality_df = execution_quality_node.fit(
    simulated_and_actual_trade_price_df
)["df_out"]

# %%
sim_vs_actual_execution_quality_df.columns.levels[0].to_list()

# %%
oms.get_asset_slice(sim_vs_actual_execution_quality_df, btc_usdt_id)

# %%
col = "buy_trade_slippage_bps"
coplotti.plot_slippage_boxplot(
    sim_vs_actual_execution_quality_df[col], "by_asset", ylabel=col
)

# %%
sim_vs_actual_execution_quality_df["buy_trade_slippage_bps"].unstack().hist(
    bins=31
)

# %%
col = "sell_trade_slippage_bps"
coplotti.plot_slippage_boxplot(
    sim_vs_actual_execution_quality_df[col], "by_asset", ylabel=col
)

# %%
sim_vs_actual_execution_quality_df["sell_trade_slippage_bps"].unstack().hist(
    bins=31
)


# %% [markdown]
# # Simulate limit order generation and trades
#
# - Specify buy/sell aggressiveness parameters
# - Specify repricing frequency and time-in-force
# - Simulate average execution prices and percentage of bars filled

# %%
def get_data():
    # Shift historical 60 seconds (bar convention)
    return bad.shift(60)


# %%
dag_builder = dtfpexexpi.ExecutionPipeline()
#
config = dag_builder.get_config_template()
# Set up `overwrite` mode to allow reassignment of values.
# Note: by default the `update_mode` does not allow overwrites,
# but they are required by the FeaturePipeline.
config.update_mode = "overwrite"
_LOG.debug("config from dag_builder=%s", config)
# Initialize config.
config["load_data"] = cconfig.Config.from_dict(
    {
        "source_node_name": "FunctionDataSource",
        "source_node_kwargs": {
            "func": get_data,
        },
    }
)
config["resample", "transformer_kwargs", "rule"] = "1T"
config["resample", "transformer_kwargs", "resample_kwargs", "offset"] = "8s"
config[
    "generate_limit_orders", "transformer_kwargs", "buy_spread_frac_offset"
] = 0.45
config[
    "generate_limit_orders", "transformer_kwargs", "sell_spread_frac_offset"
] = -0.45
config["generate_limit_orders", "transformer_kwargs", "subsample_freq"] = "60s"
config["generate_limit_orders", "transformer_kwargs", "freq_offset"] = "9s"
config["generate_limit_orders", "transformer_kwargs", "ffill_limit"] = 59

#
_LOG.debug("config after patching=%s", config)
dag = dag_builder.get_dag(config)

# %%
bid_ask_sim_flow_df = dag.run_leq_node(
    "compute_trade_vs_limit_execution_quality", "fit"
)["df_out"]

# %%
bid_ask_sim_flow_df.columns.levels[0].to_list()

# %%
btc_df = oms.get_asset_slice(bid_ask_sim_flow_df, btc_usdt_id)

# %%
btc_df[
    [
        "bid_price",
        "ask_price",
        "buy_order_num",
        "buy_trade_price",
        "buy_limit_order_price",
    ]
]

# %%
bid_ask_sim_flow_df.head()

# %%
oms.get_asset_slice(bid_ask_sim_flow_df, btc_usdt_id)[
    ["buy_trade_price", "sell_trade_price"]
].plot()

# %%
oms.get_asset_slice(bid_ask_sim_flow_df, btc_usdt_id).head()

# %%
btc_simulated_prices = oms.get_asset_slice(bid_ask_sim_flow_df, btc_usdt_id)

# %%
active_cols = ["buy_limit_order_price", "buy_trade_price"]

# %%
btc_simulated_prices[active_cols].dropna(how="all").plot()

# %%
# col = "buy_trade_midpoint_slippage_bps"
# coplotti.plot_slippage_boxplot(df_out[col], "by_time", ylabel=col)

# %%
# col = "sell_trade_midpoint_slippage_bps"
# coplotti.plot_slippage_boxplot(df_out[col], "by_time", ylabel=col)

# %%
costatis.compute_moments(
    bid_ask_sim_flow_df["buy_trade_limit_slippage_bps"].unstack()
)

# %%
costatis.compute_moments(
    bid_ask_sim_flow_df["sell_trade_limit_slippage_bps"].unstack()
)

# %% [markdown]
# # Compare simulated limit orders and actual limit orders

# %%
simulated_buy_limits = bid_ask_sim_flow_df["buy_limit_order_price"]

# %%
simulated_buy_limits.head()

# %%
actual_buy_limits = in_df["buy_limit_order_price"]

# %%
actual_buy_limits.head()

# %%
lim_vs_lim = pd.concat(
    {
        "simulated_buy_limit": simulated_buy_limits,
        "actual_buy_limit": actual_buy_limits.resample(
            "1T", offset="8s", closed="right", label="right"
        ).mean(),
    },
    axis=1,
)

# %%
lim_vs_lim.columns.levels[1]

# %%
oms.get_asset_slice(lim_vs_lim, lim_vs_lim.columns.levels[1][3]).dropna(
    how="all"
).plot()

# %%
oms.get_asset_slice(lim_vs_lim, lim_vs_lim.columns.levels[1][0]).dropna(
    how="all"
).plot()

# %%
