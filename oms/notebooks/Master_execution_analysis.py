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
# Analyze trading execution quality.

# %%
# %load_ext autoreload
# %autoreload 2

import logging
import os

import numpy as np
import pandas as pd

import core.finance as cofinanc
import core.plotting as coplotti
import core.statistics as costatis
import dataflow.core as dtfcore
import dataflow.system as dtfsys
import dataflow_amp.system.Cx as dtfamsysc
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hprint as hprint
import im_v2.common.universe as ivcu
import market_data as mdata
import oms.ccxt.ccxt_filled_orders_reader as occforre
import oms.reconciliation as omreconc
import oms.target_position_and_order_generator as otpaorge

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %% [markdown]
# # Config

# %%
# system_log_dir = "/shared_data/system_log_dir_20230323_20minutes/"
# system_log_dir = "/shared_data/ecs/preprod/twap_experiment/system_reconciliation/C3a/20230412/system_log_dir.scheduled.20230412_041000.20230412_080500"
# system_log_dir = "/shared_data/ecs/preprod/twap_experiment/system_reconciliation/C3a/20230413/system_log_dir.scheduled.20230413_143500.20230413_203000"
system_log_dir = "/app/system_log_dir/"
bar_duration = "5T"
id_col = "asset_id"
universe_version = "v7.1"

# %%
ccxt_log_reader = occforre.CcxtLogsReader(system_log_dir)

# %%
btc_ustd_id = 1467591036

# %%
use_historical = True

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
execution_has_btc = btc_ustd_id in fills_df["asset_id"]
_LOG.info("execution_has_btc=%s", execution_has_btc)

# %%
child_order_df.head(3)

# %%
child_order_df.info()

# %% [markdown]
# ## Aggregate by order and bar

# %%
# TODO(Paul): Look into adding tqdm.
# Aggregate order responses by bar.
bar_ccxt_order_aggregation = occforre.aggregate_ccxt_orders_by_bar(
    ccxt_order_response_df, bar_duration
)
# Aggregate fills by order.
ccxt_order_fills = occforre.aggregate_fills_by_order(fills_df)
# Aggregate fills by bar.
bar_fills = occforre.aggregate_fills_by_bar(
    fills_df, bar_duration, groupby_id_col=id_col
)
# Aggregate buy/sell trade prices by bar.
trade_prices = occforre.compute_buy_sell_prices_by_bar(
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
if execution_has_btc:
    omreconc.get_asset_slice(bar_ccxt_order_aggregation, btc_asset_id)

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
inconsistent_order_num_df = bar_ccxt_order_aggregation["order_count"].stack()[
    ~bar_ccxt_order_aggregation["order_count"].stack().isin([0, 4])
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
        & (child_order_df["asset_id"].isin(inconsistent_order_num_df.index[1]))
    ]

# %% [markdown]
# #### Check child orders with no corresponding exchange responses

# %%
_LOG.info(
    f"child orders in child_order_df={child_order_df.shape[0]}\nresponses in ccxt_order_response_df={ccxt_order_response_df.shape[0]}\n\
child_order_df-ccxt_order_response_df={child_order_df.shape[0]-ccxt_order_response_df.shape[0]}"
)

# %%
# Get child orders that were generated but did not get an order response.
no_response_orders = child_order_df[child_order_df["ccxt_id"] == -1]
no_response_orders["error_msg"] = no_response_orders["extra_params"].apply(
    lambda x: eval(x).get("error_msg", "")
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
# coplotti.plot_slippage_boxplot(bar_ccxt_order_aggregation[col], "by_time", ylabel=col)

# %%
# TODO(Paul): This plotting function applies here too, even though we are working with counts
#  instead of slippage. Rename and move the plotting function.
# col = "order_count"
# coplotti.plot_slippage_boxplot(bar_ccxt_order_aggregation[col], "by_asset", ylabel=col)

# %% [markdown]
# ## Fee summary

# %%
order_fees = fills_df["transaction_cost"].sum()
_LOG.info("Cumulative order fees in dollars=%f" % order_fees)

# %%
traded_dollar_volume = fills_df["cost"].sum()
_LOG.info("Cumulative turnover in dollars=%f", traded_dollar_volume)

# %%
order_fees_in_bps = 1e4 * order_fees / traded_dollar_volume
_LOG.info("Cumulative order fees in bps=%f", order_fees_in_bps)

# %%
realized_pnl = fills_df["realized_pnl"].sum()
_LOG.info("Realized gross pnl in dollars=%f", realized_pnl)

# %%
realized_pnl_in_bps = 1e4 * realized_pnl / traded_dollar_volume
_LOG.info("Realized gross pnl in bps=%f", realized_pnl_in_bps)

# %% [markdown]
# ## Align ccxt orders and fills

# %%
filled_ccxt_orders, unfilled_ccxt_orders = occforre.align_ccxt_orders_and_fills(
    ccxt_order_response_df, fills_df
)

# %%
filled_ccxt_orders.head(3)

# %%
unfilled_ccxt_orders.head(3)

# %%
filled_order_execution_quality = occforre.compute_filled_order_execution_quality(
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
bar_child_order_aggregation = occforre.aggregate_child_limit_orders_by_bar(
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
start_timestamp = bar_fills["first_datetime"].min()
_LOG.info("start_timestamp=%s", start_timestamp)
end_timestamp = bar_fills["last_datetime"].max()
_LOG.info("end_timestamp=%s", end_timestamp)


# %%
def _get_prod_market_data(universe_version: str) -> mdata.MarketData:
    """
    Get `MarketData` backed by the realtime prod DB.

    :param universe version: universe version, e.g., "v7.1."
    """
    # Get trading universe as asset ids.
    vendor = "CCXT"
    mode = "trade"
    as_full_symbol = True
    full_symbols = ivcu.get_vendor_universe(
        vendor,
        mode,
        version=universe_version,
        as_full_symbol=as_full_symbol,
    )
    asset_ids = [
        ivcu.string_to_numerical_id(full_symbol) for full_symbol in full_symbols
    ]
    # Get prod `MarketData`.
    market_data = dtfamsysc.get_Cx_RealTimeMarketData_prod_instance1(asset_ids)
    return market_data


def load_and_resample_ohlcv_data(
    start_timestamp: pd.Timestamp,
    end_timestamp: pd.Timestamp,
    bar_duration: str,
    universe_version: str,
) -> pd.DataFrame:
    """
    Load OHLCV data and resample it.

    :param start_timestamp: the earliest date timestamp to load data for
    :param end_timestamp: the latest date timestamp to load data for
    :param bar_duration: bar duration as pandas string
    :param universe_version: universe version, e.g., "v7.1."
    """
    nid = "read_data"
    market_data = _get_prod_market_data(universe_version)
    ts_col_name = "end_timestamp"
    multiindex_output = True
    col_names_to_remove = None
    # This is similar to what `RealTimeDataSource` does in production
    # but allows to query data in the past.
    historical_data_source = dtfsys.HistoricalDataSource(
        nid,
        market_data,
        ts_col_name,
        multiindex_output,
        col_names_to_remove=col_names_to_remove,
    )
    # Convert to the DataFlow `Intervals` format.
    fit_intervals = [(start_timestamp, end_timestamp)]
    _LOG.info("fit_intervals=%s", fit_intervals)
    historical_data_source.set_fit_intervals(fit_intervals)
    df_ohlcv = historical_data_source.fit()["df_out"]
    # Resample data.
    resampling_node = dtfcore.GroupedColDfToDfTransformer(
        "resample",
        transformer_func=cofinanc.resample_bars,
        **{
            "in_col_groups": [
                ("open",),
                ("high",),
                ("low",),
                ("close",),
                ("volume",),
            ],
            "out_col_group": (),
            "transformer_kwargs": {
                "rule": bar_duration,
                "resampling_groups": [
                    ({"close": "close"}, "last", {}),
                    ({"high": "high"}, "max", {}),
                    ({"low": "low"}, "min", {}),
                    ({"open": "open"}, "first", {}),
                    (
                        {"volume": "volume"},
                        "sum",
                        {"min_count": 1},
                    ),
                    (
                        {
                            "close": "twap",
                        },
                        "mean",
                        {},
                    ),
                ],
                "vwap_groups": [
                    ("close", "volume", "vwap"),
                ],
            },
            "reindex_like_input": False,
            "join_output_with_input": False,
        },
    )
    resampled_ohlcv = resampling_node.fit(df_ohlcv)["df_out"]
    return resampled_ohlcv


# %%
ohlcv_bars = load_and_resample_ohlcv_data(
    start_timestamp,
    end_timestamp,
    bar_duration,
    universe_version,
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
actual_vs_ohlcv_execution_df = occforre.compute_execution_quality(
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
actual_vs_ohlcv_execution_df["buy_trade_slippage_bps"].unstack().hist(bins=31)

# %%
actual_vs_ohlcv_execution_df["sell_trade_slippage_bps"].unstack().hist(bins=31)

# %%
if execution_has_btc:
    omreconc.get_asset_slice(actual_and_ohlcv_price_df, btc_usdt_id).plot()

# %%
col = "buy_trade_slippage_bps"
coplotti.plot_slippage_boxplot(
    actual_vs_ohlcv_execution_df[col], "by_time", ylabel=col
)

# %%
coplotti.plot_slippage_boxplot(
    actual_vs_ohlcv_execution_df[col], "by_asset", ylabel=col
)

# %%
col = "sell_trade_slippage_bps"
coplotti.plot_slippage_boxplot(
    actual_vs_ohlcv_execution_df[col], "by_time", ylabel=col
)

# %%
coplotti.plot_slippage_boxplot(
    actual_vs_ohlcv_execution_df[col], "by_asset", ylabel=col
)
