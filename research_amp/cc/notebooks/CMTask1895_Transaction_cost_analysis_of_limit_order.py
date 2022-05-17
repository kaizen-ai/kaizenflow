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
# # Imports

# %%
# %load_ext autoreload
# %autoreload 2


from datetime import timedelta

import pandas as pd
import seaborn as sns

import core.plotting.normality as cplonorm
import research_amp.transform as ramptran

# %% [markdown]
# # Load the data

# %% run_control={"marked": false}
# Read saved 1 month of data.
ohlcv_cc = pd.read_csv("/shared_data/cc_ohlcv.csv", index_col="timestamp")
btc_ohlcv = ohlcv_cc[ohlcv_cc["full_symbol"] == "binance::BTC_USDT"]
btc_ohlcv.index = pd.to_datetime(btc_ohlcv.index)
ohlcv_cols = [
    "open",
    "high",
    "low",
    "close",
    "volume",
]
btc_ohlcv = btc_ohlcv[ohlcv_cols]
btc_ohlcv.head(3)

# %% run_control={"marked": false}
# Read saved 1 month of data.
bid_ask_btc = pd.read_csv(
    "/shared_data/bid_ask_btc_jan22_1min_last.csv", index_col="timestamp"
)
bid_ask_btc.index = pd.to_datetime(bid_ask_btc.index)

# Read from crypto_chassis directly.
# Specify the params.
# full_symbols = ["binance::BTC_USDT"]
# start_date = pd.Timestamp("2022-01-01", tz="UTC")
# end_date = pd.Timestamp("2022-02-01", tz="UTC")
# Get the data.
# bid_ask_df = raccchap.read_and_resample_bid_ask_data(
#   full_symbols, start_date, end_date, "1T"
# )
bid_ask_btc.head(3)

# %%
# Transform the data.
bid_ask_btc.index = pd.to_datetime(bid_ask_btc.index)
# Compute bid ask stats.
bid_ask_btc = ramptran.calculate_bid_ask_statistics(bid_ask_btc)
# Choose only necessary values.
bid_ask_btc = bid_ask_btc.swaplevel(axis=1)["binance::BTC_USDT"][
    ["bid_size", "ask_size", "bid_price", "ask_price", "mid", "quoted_spread"]
]
bid_ask_btc.index = bid_ask_btc.index.shift(-1, freq="T")
bid_ask_btc

# %%
# OHLCV + bid ask
btc = pd.concat([btc_ohlcv, bid_ask_btc], axis=1)
btc.head(3)

# %% run_control={"marked": false}
close_outside_ba = len(
    btc.loc[(btc["close"] > btc["ask_price"]) | (btc["close"] < btc["bid_price"])]
)
print(
    f"Share of close prices outside bid-ask spread: %.3f"
    % (close_outside_ba / len(btc))
)
btc[["close", "bid_price", "ask_price"]].head(100).plot(figsize=(15, 7))


# %% [markdown]
# # Analysis

# %% run_control={"marked": false}
def compute_execution(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute execution statistics in the trade interval for a given limit order.

    :param df: data of time interval for trading period.
    """
    result = {}
    # Execution is triggered if `limit_price` is inside [low,high] interval of close price.
    # It is guarantee that there was execution in [low, high] of close price, and thus our
    # order would have triggered.
    # TODO(gp): more precisely we should use [low, high] of ask / bid (depending on the direction)
    # to trigger an execution.
    df["execution_is_triggered"] = (df["high"] >= df["limit_price"]) & (
        df["low"] <= df["limit_price"]
    )
    # Execution volume is the volume that was available in the bars where there was execution.
    # TODO(gp): more precisely we should use the ask / bid size (depending on the direction).
    result["executed_volume"] = (
        df["ask_size"] * df["execution_is_triggered"]
    ).sum()
    # The notional executed volume is the amount of dollars we could have executed assuming to sweep the first level of the order book.
    result["executed_notional"] = (df["ask_size"] * df["limit_price"]).sum()
    # In how many bars there was execution.
    result["executed_bars"] = df["execution_is_triggered"].sum()
    # Average price for which there was execution.
    result["executed_avg_price"] = (
        df["limit_price"] * df["execution_is_triggered"]
    ).mean()
    # Build a df with all the results and mark it at the end of the interval.
    result = pd.DataFrame(result, index=[df.index[-1]])
    return result


def compute_transaction_cost(
    df, start_time, interval_in_mins=10, trade_in_mins=5, is_buy=True
) -> pd.DataFrame:
    """
    Given prices and ask / bids in the bars (a, b] marked at the end of the
    interval, compute the cost to enter a buy / sell trade in the interval
    [start_time, start_time + interval_in_mins) and the pnl from holding
    between.
    """
    # hdbg.dassert_lt(trade_in_mins, intervals_in_mins)
    # The position is entered in the interval:
    # [start_time, start_time + trade_in_mins)
    trade_in_mins = timedelta(minutes=trade_in_mins)
    trade_interval = df.loc[
        start_time : start_time + trade_in_mins - timedelta(minutes=1)
    ].copy()
    # Add "limit_price" to df.
    if is_buy:
        trade_interval["limit_price"] = (
            trade_interval.loc[start_time]["ask_price"] - 0.01
        )
    else:
        trade_interval["limit_price"] = (
            trade_interval.loc[start_time]["bid_price"] + 0.01
        )
    result = compute_execution(trade_interval)
    # The position is held during the interval:
    # [start_time + trade_in_mins, start_time + interval_in_mins)
    interval_in_mins = (
        timedelta(minutes=interval_in_mins) + trade_in_mins - timedelta(minutes=1)
    )
    start_price = df.loc[start_time + trade_in_mins]["close"]
    end_price = df.loc[start_time + interval_in_mins]["close"]
    if is_buy:
        trade_pnl_per_share = start_price - end_price
    else:
        trade_pnl_per_share = -(start_price - end_price)
    result["close_price_holding_period"] = end_price
    result["trade_pnl_per_share"] = trade_pnl_per_share
    result["holding_notional"] = (
        result["close_price_holding_period"] * result["executed_volume"]
    )
    result["notional_diff"] = (
        result["holding_notional"] - result["executed_notional"]
    )
    return result


# %%
num_intervals = (btc.index.max() - btc.index.min()).total_seconds() / (15 * 60)
num_intervals = range(int(num_intervals))
num_intervals

# %%
start_time = btc.index.min()
final_result = []
for i in num_intervals:
    result_tmp = compute_transaction_cost(btc, start_time)
    final_result.append(result_tmp)
    start_time = start_time + timedelta(minutes=15)
final_result = pd.concat(final_result)
final_result.head(3)

# %% [markdown]
# # The distribution of money made - money spent

# %%
post_results = final_result.copy()
post_results["notional_diff"].describe()

# %%
sns.displot(post_results, x="notional_diff")

# %%
# Visualize the result
cplonorm.plot_qq(post_results["notional_diff"])
