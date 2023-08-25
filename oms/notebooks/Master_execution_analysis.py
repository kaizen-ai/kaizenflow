# ---
# jupyter:
#   jupytext:
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
#
# Analyze trading execution quality.

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
import dataflow_amp.system.Cx as dtfamsysc
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hpandas as hpandas
import helpers.hprint as hprint
import im_v2.common.universe as ivcu
import oms
import oms.broker.ccxt.ccxt_aggregation_functions as obccagfu
import oms.broker.ccxt.ccxt_execution_quality as obccexqu
import oms.broker.ccxt.ccxt_logs_reader as obcclore
import oms.reconciliation as omreconc

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %% [markdown]
# # Config

# %%
config = cconfig.get_config_from_env()
if config:
    # Get config from env when running the notebook via the `run_notebook.py` script, e.g.,
    # in the system reconciliation flow.
    _LOG.info("Using config from env vars")
else:
    system_log_dir = "/shared_data/ecs/test/twap_experiment/20230815_1"
    id_col = "asset_id"
    universe_version = "v7.1"
    vendor = "CCXT"
    mode = "trade"
    test_asset_id = 1467591036
    bar_duration = "5T"
    expected_num_child_orders = [0, 5]
    use_historical = True
    config_dict = {
        "meta": {"id_col": id_col, "use_historical": use_historical},
        "system_log_dir": system_log_dir,
        "ohlcv_market_data": {
            "vendor": vendor,
            "mode": mode,
            "universe": {
                "universe_version": universe_version,
                "test_asset_id": test_asset_id,
            },
        },
        "execution_parameters": {
            "bar_duration": bar_duration,
            "expected_num_child_orders_per_bar": expected_num_child_orders,
        },
    }
    config = cconfig.Config.from_dict(config_dict)
print(config)

# %%
ccxt_log_reader = obcclore.CcxtLogsReader(
    config.get_and_mark_as_used(("system_log_dir",))
)

# %% [markdown]
# # Load Ccxt order responses and fills

# %%
data = ccxt_log_reader.load_all_data()
ccxt_order_response_df = data["ccxt_order_responses"]
fills_df = data["ccxt_trades"]
child_order_df = data["oms_child_orders"]

# %%
ccxt_order_response_df.head(3)

# %%
ccxt_order_response_df.info()

# %%
fills_df.head(3)

# %%
fills_df.info()

# %%
test_asset_id = config.get_and_mark_as_used(
    ("ohlcv_market_data", "universe", "test_asset_id")
)
id_col = config.get_and_mark_as_used(("meta", "id_col"))
hdbg.dassert_in(test_asset_id, fills_df[id_col].to_list())

# %%
child_order_df.head(3)

# %%
child_order_df.info()

# %% [markdown]
# ## Aggregate by order and bar

# %%
bar_duration = config.get_and_mark_as_used(
    ("execution_parameters", "bar_duration")
)
# TODO(Paul): Look into adding tqdm.
# Aggregate order responses by bar.
bar_ccxt_order_aggregation = obccagfu.aggregate_ccxt_orders_by_bar(
    ccxt_order_response_df, bar_duration
)
# Aggregate fills by order.
ccxt_order_fills = obccagfu.aggregate_fills_by_order(fills_df)
# Aggregate fills by bar.
bar_fills = obccagfu.aggregate_fills_by_bar(
    fills_df, bar_duration, groupby_id_col=id_col
)
# Aggregate buy/sell trade prices by bar.
trade_prices = obccagfu.compute_buy_sell_prices_by_bar(
    fills_df, bar_duration, groupby_id_col=id_col
)

# %%
bar_ccxt_order_aggregation.head(3)

# %%
ccxt_order_fills.head(3)

# %%
bar_fills.head(3)

# %%
trade_prices.head(3)

# %%
omreconc.get_asset_slice(bar_ccxt_order_aggregation, test_asset_id)

# %%
# If `order_twap` and `order_vwap` are different for a given instrument and bar,
#  then we are likely submitting orders of differing sizes.
# Use rounding to ignore machine precision artifacts.
#
# If this number is not zero, then closely inspect the dataframe (without summing
# absolute values).
bar_ccxt_order_aggregation["buy_limit_twap"].subtract(
    bar_ccxt_order_aggregation["buy_limit_vwap"]
).abs().sum().sum().round(9)

# %%
# Analogous check but for sells.
bar_ccxt_order_aggregation["sell_limit_twap"].subtract(
    bar_ccxt_order_aggregation["sell_limit_vwap"]
).abs().sum().sum().round(9)

# %% [markdown]
# ### Cross-checks

# %%
# For a given bar and a given instrument, we expect to see either 0 (child) orders, or X child orders,
# where X is constant across the run.
# This should have counts for two values: zero and the number X of trades per bar.
bar_ccxt_order_aggregation["order_count"].stack().value_counts()

# %% [markdown]
# #### Cross-checking the missing order count for bars

# %% [markdown]
# The correct number of child orders expected to be submitted for a bar is 4 per asset_id (if there is a planned submission), or 0 per asset_id (if no orders are planned to be submitted or the order notional is too small for the exchange to accept the order.

# %%
# Get all bars and asset_ids with inconsistent child order number.
expected_num_child_orders = config.get_and_mark_as_used(
    ("execution_parameters", "expected_num_child_orders_per_bar")
)
inconsistent_order_num_df = bar_ccxt_order_aggregation["order_count"].stack()[
    ~bar_ccxt_order_aggregation["order_count"]
    .stack()
    .isin(expected_num_child_orders)
]
inconsistent_order_num_df

# %% [markdown]
# - Most of the 2- and 3-children order submissions happen at the last bar, where there is a strict time cutoff which does not let the child orders to finish submission.

# %%
# Get all orders for the bars in the range where the numbers have been inconsistent.
# Note: may yield uninterpretable results if the bars are not consecutive.
if False:
    child_order_df[
        (
            child_order_df["creation_timestamp"]
            > inconsistent_order_num_df.index.min()[0]
        )
        & (
            child_order_df["creation_timestamp"]
            < inconsistent_order_num_df.index.max()[0]
        )
        & (child_order_df[id_col].isin(inconsistent_order_num_df.index[1]))
    ]

# %% [markdown]
# #### Check child orders with no corresponding exchange responses

# %%
_LOG.info(
    f"child orders in child_order_df={child_order_df.shape[0]}\nresponses in ccxt_order_response_df={ccxt_order_response_df.shape[0]}\n\
child_order_df-ccxt_order_response_df={child_order_df.shape[0]-ccxt_order_response_df.shape[0]}"
)

# %%
# Extract ccxt_id from list.
child_order_df["ccxt_id_as_single_value"] = child_order_df["ccxt_id"].apply(
    lambda x: x[0]
)
# Get child orders that were generated but did not get an order response.no_response_orders = child_order_df[child_order_df["ccxt_id"] == "-1"]
no_response_orders = child_order_df[
    child_order_df["ccxt_id_as_single_value"] == -1
]
no_response_orders["error_msg"] = no_response_orders["extra_params"].apply(
    lambda x: x.get("error_msg", "")
)

# %%
# Check the error messages for child orders that did not come through.
no_response_orders["error_msg"].value_counts()

# %%
# Not all error messages are logged as of now, check child orders with no response and no logged error message.
no_response_orders[no_response_orders["error_msg"] == ""]

# %%
# If `buy_count` and `sell_count` are both greater than zero for an order,
#  there is a problem.
has_buys = ccxt_order_fills["buy_count"] > 0
has_sells = ccxt_order_fills["sell_count"] > 0
# This dataframe should have zero rows.
ccxt_order_fills.loc[has_buys & has_sells].shape[0]

# %%
# If `buy_count` and `sell_count` are both greater than zero for an instrument and bar,
#  there is likely a bar alignment or timing problem.
has_buys = bar_fills["buy_count"] > 0
has_sells = bar_fills["sell_count"] > 0
# This dataframe should be empty.
bar_fills.loc[has_buys & has_sells].shape[0]

# %%
# TODO(Paul): This plotting function applies here too, even though we are working with counts
#  instead of slippage. Rename and move the plotting function.
# TODO(Paul): This plotting function doesn't seem to be idempotent.
# col = "order_count"
# coplotti.plot_boxplot(bar_ccxt_order_aggregation[col], "by_row", ylabel=col)

# %%
# TODO(Paul): This plotting function applies here too, even though we are working with counts
#  instead of slippage. Rename and move the plotting function.
# col = "order_count"
# coplotti.plot_boxplot(bar_ccxt_order_aggregation[col], "by_col", ylabel=col)

# %% [markdown]
# ## Fee summary

# %%
group_by_col = "is_buy"
obccexqu.generate_fee_summary(fills_df, "is_buy")

# %%
group_by_col = "is_maker"
obccexqu.generate_fee_summary(fills_df, group_by_col)

# %%
group_by_col = "is_positive_realized_pnl"
obccexqu.generate_fee_summary(fills_df, group_by_col)

# %% [markdown]
# ## Align ccxt orders and fills

# %%
filled_ccxt_orders, unfilled_ccxt_orders = obccexqu.align_ccxt_orders_and_fills(
    ccxt_order_response_df, fills_df
)

# %%
filled_ccxt_orders.head(3)

# %%
unfilled_ccxt_orders.head(3)

# %%
filled_order_execution_quality = obccexqu.compute_filled_order_execution_quality(
    filled_ccxt_orders, tick_decimals=6
)
filled_order_execution_quality.head()

# %%
# If any value is negative (up to machine precision), except for `direction`, there is a bug somewhere.
filled_order_execution_quality.loc[
    (filled_order_execution_quality.drop("direction", axis=1).round(9) < 0).any(
        axis=1
    )
].shape[0]

# %%
filled_order_execution_quality.min()

# %%
filled_order_execution_quality.max()

# %% [markdown]
# # Load internal parent and child orders
#
# These are in the internal `amp` format (not the `ccxt` format)

# %%
parent_order_df = ccxt_log_reader.load_oms_parent_order_df()

# %%
parent_order_df.head(3)

# %%
parent_order_df.info()

# %%
number_of_bars = int(
    np.ceil(
        (
            parent_order_df["end_timestamp"].max()
            - parent_order_df["start_timestamp"].min()
        )
        / bar_duration
    )
)
_LOG.info("number of bars=%d", number_of_bars)

# %%
unique_asset_id_count = len(parent_order_df["asset_id"].unique())
_LOG.info("unique asset_id count=%d", unique_asset_id_count)

# %%
# Share counts should change if there are many orders and we are getting fills.
costatis.compute_moments(parent_order_df["curr_num_shares"])

# %%
child_order_df = ccxt_log_reader.load_oms_child_order_df()

# %%
child_order_df.head()

# %%
child_order_df.info()

# %%
# The number of internal child orders should equal the number of ccxt order responses.
# (The difference should be zero.)
_LOG.info("Num child orders=%d", child_order_df.shape[0])
_LOG.info(
    "Num ccxt order responses=%d", ccxt_order_response_df["order"].nunique()
)
_LOG.info(
    "Num child orders minus num ccxt order responses=%d",
    child_order_df.shape[0] - ccxt_order_response_df["order"].nunique(),
)

# %%
bar_child_order_aggregation = obccagfu.aggregate_child_limit_orders_by_bar(
    child_order_df, bar_duration
)

# %%
bar_child_order_aggregation.head(3)

# %%
bar_child_order_aggregation.columns.levels[0].to_list()

# %% [markdown]
# # Load OHLCV data

# %%
# TODO(Paul): Refine the cuts around the first and last bars.
start_timestamp = bar_fills["first_datetime"].min() - pd.Timedelta(bar_duration)
_LOG.info("start_timestamp=%s", start_timestamp)
end_timestamp = bar_fills["last_datetime"].max()
_LOG.info("end_timestamp=%s", end_timestamp)

# %%
universe_version = config.get_and_mark_as_used(
    ("ohlcv_market_data", "universe", "universe_version")
)
vendor = config.get_and_mark_as_used(
    (
        "ohlcv_market_data",
        "vendor",
    )
)
mode = config.get_and_mark_as_used(
    (
        "ohlcv_market_data",
        "mode",
    )
)
# Get asset ids.
asset_ids = ivcu.get_vendor_universe_as_asset_ids(universe_version, vendor, mode)
# Get prod `MarketData`.
market_data = dtfamsysc.get_Cx_RealTimeMarketData_prod_instance1(asset_ids)
# Load and resample OHLCV data.
ohlcv_bars = dtfamsysc.load_and_resample_ohlcv_data(
    market_data,
    start_timestamp,
    end_timestamp,
    bar_duration,
)

# %%
ohlcv_bars.head(3)

# %% [markdown]
# # Compare bar trade prices to OHLCV TWAP

# %%
actual_and_ohlcv_price_df = {
    "buy_trade_price": trade_prices["buy_trade_price"],
    "sell_trade_price": trade_prices["sell_trade_price"],
    "twap": ohlcv_bars["twap"],
    "high": ohlcv_bars["high"],
    "low": ohlcv_bars["low"],
}
actual_and_ohlcv_price_df = pd.concat(actual_and_ohlcv_price_df, axis=1)
actual_and_ohlcv_price_df.head()

# %%
actual_vs_ohlcv_execution_df = cofinanc.compute_ref_price_execution_quality(
    actual_and_ohlcv_price_df,
    "twap",
    "twap",
    "buy_trade_price",
    "sell_trade_price",
)
actual_vs_ohlcv_execution_df.head()

# %%
actual_vs_ohlcv_execution_df.columns.levels[0].to_list()

# %%
buy_trade_slippage_bps = (
    actual_vs_ohlcv_execution_df["buy_trade_slippage_bps"]
    .unstack()
    .rename("buy_trade_slippage_bps")
)
sell_trade_slippage_bps = (
    actual_vs_ohlcv_execution_df["sell_trade_slippage_bps"]
    .unstack()
    .rename("sell_trade_slippage_bps")
)
buy_sell_trade_slippage_bps_ecdf = costatis.compute_and_combine_empirical_cdfs(
    [buy_trade_slippage_bps, sell_trade_slippage_bps]
)
buy_sell_trade_slippage_bps_ecdf.plot(yticks=np.arange(0, 1.1, 0.1))

# %%
omreconc.get_asset_slice(actual_and_ohlcv_price_df, test_asset_id).plot()

# %%
col = "buy_trade_slippage_bps"
coplotti.plot_boxplot(actual_vs_ohlcv_execution_df[col], "by_row", ylabel=col)

# %%
coplotti.plot_boxplot(actual_vs_ohlcv_execution_df[col], "by_col", ylabel=col)

# %%
col = "sell_trade_slippage_bps"
coplotti.plot_boxplot(actual_vs_ohlcv_execution_df[col], "by_row", ylabel=col)

# %%
coplotti.plot_boxplot(actual_vs_ohlcv_execution_df[col], "by_col", ylabel=col)

# %% [markdown]
# # Construct portfolio, target positions and compute execution quality

# %% [markdown]
# ## Compute `target_position_df`

# %%
target_position_df = obccexqu.convert_parent_orders_to_target_position_df(
    parent_order_df,
    ohlcv_bars["twap"],
)

# %%
fills = oms.compute_fill_stats(target_position_df)
hpandas.df_to_str(fills, num_rows=5, log_level=logging.INFO)

# %%
col = "fill_rate"
coplotti.plot_boxplot(fills[col], "by_row", ylabel=col)

# %%
col = "fill_rate"
coplotti.plot_boxplot(fills[col], "by_col", ylabel=col)

# %% [markdown]
# ## Compute `portfolio_df`

# %%
portfolio_df = obccexqu.convert_bar_fills_to_portfolio_df(
    bar_fills,
    ohlcv_bars["twap"],
)

# %%
portfolio_df.head()

# %%
slippage = oms.compute_share_prices_and_slippage(portfolio_df)
hpandas.df_to_str(slippage, num_rows=5, log_level=logging.INFO)
slippage["slippage_in_bps"].plot()

# %%
stacked = slippage[["slippage_in_bps", "is_benchmark_profitable"]].stack()
slippage_when_benchmark_profitable = stacked[
    stacked["is_benchmark_profitable"] > 0
]["slippage_in_bps"].rename("slippage_when_benchmark_profitable")
slippage_when_not_benchmark_profitable = stacked[
    stacked["is_benchmark_profitable"] <= 0
]["slippage_in_bps"].rename("slippage_when_not_benchmark_profitable")
slippage_when_benchmark_profitable.hist(bins=31, edgecolor="black", color="green")
slippage_when_not_benchmark_profitable.hist(
    bins=31, alpha=0.7, edgecolor="black", color="red"
)

# %%
slippage_benchmark_profitability_ecdfs = (
    costatis.compute_and_combine_empirical_cdfs(
        [
            slippage_when_benchmark_profitable,
            slippage_when_not_benchmark_profitable,
        ]
    )
)
slippage_benchmark_profitability_ecdfs.plot(yticks=np.arange(0, 1.1, 0.1))

# %%
# TODO(*): Clean up and factor out.
maker_ratio_df = bar_fills.copy()
maker_ratio_df["maker_ratio"] = maker_ratio_df["maker_count"] / (
    maker_ratio_df["maker_count"] + maker_ratio_df["taker_count"]
)
maker_ratio_df = maker_ratio_df["maker_ratio"].unstack(level=1)
maker_ratio_df = pd.concat({"maker_ratio": maker_ratio_df}, axis=1)
slippage2 = pd.concat([slippage, maker_ratio_df], axis=1)
stacked2 = slippage2[["slippage_in_bps", "maker_ratio"]].stack()
slippage_maker_dominated = stacked2[stacked2["maker_ratio"] > 0.5][
    "slippage_in_bps"
].rename("slippage_maker_dominated")
slippage_taker_dominated = stacked2[stacked2["maker_ratio"] <= 0.5][
    "slippage_in_bps"
].rename("slippage_taker_dominated")
slippage_maker_taker_ecdfs = costatis.compute_and_combine_empirical_cdfs(
    [slippage_maker_dominated, slippage_taker_dominated]
)
slippage_maker_taker_ecdfs.plot(yticks=np.arange(0, 1.1, 0.1))

# %% [markdown]
# ## Compute notional costs

# %%
notional_costs = omreconc.compute_notional_costs(
    portfolio_df,
    target_position_df,
)
hpandas.df_to_str(notional_costs, num_rows=5, log_level=logging.INFO)

# %%
omreconc.summarize_notional_costs(notional_costs, "by_bar").plot(kind="bar")

# %%
omreconc.summarize_notional_costs(notional_costs, "by_asset").plot(kind="bar")

# %%
omreconc.summarize_notional_costs(notional_costs, "by_bar").sum()

# %%
portfolio_stats_df = cofinanc.compute_bar_metrics(
    portfolio_df["holdings_notional"],
    -portfolio_df["executed_trades_notional"],
    portfolio_df["pnl"],
    compute_extended_stats=True,
)

# %%
portfolio_stats_df.head()

# %%
coplotti.plot_portfolio_stats(portfolio_stats_df)

# %% [markdown]
# # Config after notebook run

# %%
print(config.to_string(mode="verbose"))
