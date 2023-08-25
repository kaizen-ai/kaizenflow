"""
Import as:

import oms.broker.ccxt.ccxt_aggregation_functions as obccagfu
"""
import logging

import numpy as np
import pandas as pd

import helpers.hdbg as hdbg
import im_v2.common.universe.universe_utils as imvcuunut

_LOG = logging.getLogger(__name__)


# #############################################################################
# CCXT order response processing
# #############################################################################


def aggregate_ccxt_orders_by_bar(
    df: pd.DataFrame,
    freq: str,
) -> pd.DataFrame:
    """
    Summarize ccxt order responses by bar and instrument.

    :param df: result of `load_ccxt_order_response_df()`
    :param freq: bar duration as a string, e.g, `"5T"`
    :return: DataFrame with 2 col levels, with various order stats
        organized by asset and by bar (a timestamp index)
    """
    df = df.set_index("order")
    df["bar_start_datetime"] = df["order_update_datetime"].dt.floor(freq)
    df["bar_end_datetime"] = df["order_update_datetime"].dt.ceil(freq)
    # Convert CCXT full symbol to full symbol format.
    # There are 2 CCXT symbol formats, e.g.'GMT/USDT:USDT' for futures
    # and 'GMT/USDT' for spot trades and older versions of CCXT.
    df["full_symbol"] = df["symbol"].apply(lambda x: x.split(":")[0])
    df["full_symbol"] = (
        "binance" + "::" + df["full_symbol"].apply(lambda x: x.replace("/", "_"))
    )
    # Convert resulting full symbols to asset ids.
    df_symbols = df["full_symbol"].unique()
    asset_id_to_full_symbol = imvcuunut.build_numerical_to_string_id_mapping(
        df_symbols
    )
    full_symbol_to_asset_id = {v: k for k, v in asset_id_to_full_symbol.items()}
    df["asset_id"] = df["full_symbol"].apply(lambda x: full_symbol_to_asset_id[x])
    #
    buy_orders = df[df["side"] == "buy"]
    sell_orders = df[df["side"] == "sell"]
    #
    buy_orders["buy_notional"] = (
        buy_orders["order_amount"] * buy_orders["order_price"]
    )
    sell_orders["sell_notional"] = (
        sell_orders["order_amount"] * sell_orders["order_price"]
    )
    #
    buy_groupby = buy_orders.groupby(["bar_end_datetime", "asset_id"])
    sell_groupby = sell_orders.groupby(["bar_end_datetime", "asset_id"])
    #
    buy_order_count = (
        buy_groupby["client_order_id"]
        .count()
        .unstack()
        .fillna(0)
        .astype(np.int64)
    )
    sell_order_count = (
        sell_groupby["client_order_id"]
        .count()
        .unstack()
        .fillna(0)
        .astype(np.int64)
    )
    order_count = buy_order_count.add(sell_order_count, fill_value=0)
    #
    buy_limit_twap = buy_groupby["order_price"].mean().unstack()
    sell_limit_twap = sell_groupby["order_price"].mean().unstack()
    #
    buy_amount = buy_groupby["order_amount"].sum().unstack()
    sell_amount = sell_groupby["order_amount"].sum().abs().unstack()
    #
    buy_notional = buy_groupby["buy_notional"].sum().unstack()
    sell_notional = sell_groupby["sell_notional"].sum().unstack()
    #
    buy_limit_vwap = buy_notional / buy_amount
    sell_limit_vwap = sell_notional / sell_amount
    #
    out_df = {
        "order_count": order_count,
        "buy_order_count": buy_order_count,
        "buy_limit_twap": buy_limit_twap,
        "buy_limit_vwap": buy_limit_vwap,
        "buy_amount": buy_amount,
        "buy_notional": buy_notional,
        "sell_order_count": sell_order_count,
        "sell_limit_twap": sell_limit_twap,
        "sell_limit_vwap": sell_limit_vwap,
        "sell_amount": sell_amount,
        "sell_notional": sell_notional,
    }
    out_df = pd.concat(out_df, axis=1)
    # # Convert the integer columns.
    # # In cases of order side alternating by bar, concatenation results in
    # # NaN values on some of the bars.
    integer_cols = ["order_count", "buy_order_count", "sell_order_count"]
    for col in integer_cols:
        # Verify that the corresponding order count is present.
        # E.g. a buy_order_count can be absent if the CCXT orders
        # were only on the 'sell' side.
        if col in out_df.columns.get_level_values(0):
            out_df[col] = out_df[col].fillna(0).astype(np.int64)
    return out_df


def aggregate_child_limit_orders_by_bar(
    df: pd.DataFrame,
    freq: str,
) -> pd.DataFrame:
    """
    Summarize OMS child orders by bar and instrument.

    This is similar to `aggregate_ccxt_orders_by_bar()`, is a separate function
    due to different column name conventions and expected fields.

    :param df: result of `load_child_order_df()`
    :param freq: bar duration as a string, e.g, `"5T"`
    :return: DataFrame with 2 col levels, with various order stats
        organized by asset and by bar (a timestamp index)
    """
    # Don't modify the DataFrame.
    df = df.copy()
    #
    df["bar_start_datetime"] = df["start_timestamp"].dt.floor(freq)
    df["bar_end_datetime"] = df["start_timestamp"].dt.ceil(freq)
    #
    buy_orders = df[df["diff_num_shares"] > 0]
    sell_orders = df[df["diff_num_shares"] < 0]
    #
    buy_orders["buy_notional"] = (
        buy_orders["diff_num_shares"] * buy_orders["limit_price"]
    )
    sell_orders["sell_notional"] = (
        sell_orders["diff_num_shares"].abs() * sell_orders["limit_price"]
    )
    #
    buy_groupby = buy_orders.groupby(["bar_end_datetime", "asset_id"])
    sell_groupby = sell_orders.groupby(["bar_end_datetime", "asset_id"])
    #
    buy_order_count = (
        buy_groupby["creation_timestamp"]
        .count()
        .unstack()
        .fillna(0)
        .astype(np.int64)
    )
    sell_order_count = (
        sell_groupby["creation_timestamp"]
        .count()
        .unstack()
        .fillna(0)
        .astype(np.int64)
    )
    order_count = pd.concat([buy_order_count, sell_order_count], axis=1)
    #
    buy_limit_twap = buy_groupby["limit_price"].mean().unstack()
    sell_limit_twap = sell_groupby["limit_price"].mean().unstack()
    #
    buy_amount = buy_groupby["diff_num_shares"].sum().unstack()
    sell_amount = sell_groupby["diff_num_shares"].sum().abs().unstack()
    #
    buy_notional = buy_groupby["buy_notional"].sum().unstack()
    sell_notional = sell_groupby["sell_notional"].sum().unstack()
    #
    buy_limit_vwap = buy_notional / buy_amount
    sell_limit_vwap = sell_notional / sell_amount
    #
    out_df = {
        "order_count": order_count,
        "buy_order_count": buy_order_count,
        "buy_limit_twap": buy_limit_twap,
        "buy_limit_vwap": buy_limit_vwap,
        "buy_amount": buy_amount,
        "buy_notional": buy_notional,
        "sell_order_count": sell_order_count,
        "sell_limit_twap": sell_limit_twap,
        "sell_limit_vwap": sell_limit_vwap,
        "sell_amount": sell_amount,
        "sell_notional": sell_notional,
    }
    out_df = pd.concat(out_df, axis=1)
    return out_df


# #############################################################################
# Fill processing
# #############################################################################


def aggregate_fills_by_bar(
    df: pd.DataFrame,
    freq: str,
    *,
    offset: str = "0T",
    groupby_id_col: str = "asset_id",
) -> pd.DataFrame:
    """
    Aggregate a fills DataFrame by order id, then by bar at `freq`.

    :param df: a fills DataFrame as returned by
        `convert_fills_json_to_dataframe()`
    :param freq: bar frequency
    """
    hdbg.dassert_in(groupby_id_col, ["asset_id", "symbol"])
    df = aggregate_fills_by_order(df)
    df["bar_start_datetime"] = df["first_datetime"].dt.floor(freq) + pd.Timedelta(
        offset
    )
    df["bar_end_datetime"] = df["first_datetime"].dt.ceil(freq) + pd.Timedelta(
        offset
    )
    # If, for a given instruments, only buys or sells occur within any given
    #  bar, then a single aggregation makes sense. Otherwise, we need to
    #  explicitly separate buys and sells.
    aggregated = df.groupby(
        ["bar_end_datetime", groupby_id_col], group_keys=True
    ).apply(_aggregate_fills)
    return aggregated


def compute_buy_sell_prices_by_bar(
    df: pd.DataFrame,
    freq: str,
    *,
    offset: str = "0T",
    groupby_id_col: str = "asset_id",
) -> pd.DataFrame:
    """
    Compute buy/sell trade prices by symbol and bar.

    :param df: a fills DataFrame as returned by
        `convert_fills_json_to_dataframe()`
    :param freq: bar frequency
    :return: DataFrame indexed by bar end datetimes and multilevel columns;
        outer level consists of "buy_trade_price", "sell_trade_price", and
        inner level consists of symbols
    """
    hdbg.dassert_in(groupby_id_col, ["asset_id", "symbol"])
    df = aggregate_fills_by_order(df)
    df["bar_end_datetime"] = df["first_datetime"].dt.ceil(freq) + pd.Timedelta(
        offset
    )
    #
    buys = df[df["buy_count"] > 0]
    sells = df[df["sell_count"] > 0]
    hdbg.dassert(buys.index.intersection(sells.index).empty)
    bar_buys = buys.groupby(
        ["bar_end_datetime", groupby_id_col], group_keys=True
    ).apply(_aggregate_fills)
    bar_sells = sells.groupby(
        ["bar_end_datetime", groupby_id_col], group_keys=True
    ).apply(_aggregate_fills)
    dict_ = {
        "buy_trade_price": bar_buys.unstack()["price"],
        "sell_trade_price": bar_sells.unstack()["price"],
    }
    df_out = pd.concat(dict_, axis=1)
    return df_out


def aggregate_fills_by_order(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate a fills DataFrame by order id.

    :param df: a fills DataFrame as returned by
        `convert_fills_json_to_dataframe()`
    """
    aggregated = (
        df.set_index(["order", "id"])
        .groupby("order", group_keys=True)
        .apply(_aggregate_fills)
    )
    return aggregated


def _aggregate_fills(df: pd.DataFrame) -> pd.Series:
    # Extract first and last fill timestamps.
    first_timestamp = df["first_timestamp"].min()
    last_timestamp = df["last_timestamp"].max()
    # Extract first and last fill datetimes.
    first_datetime = df["first_datetime"].min()
    last_datetime = df["last_datetime"].max()
    # Get symbol.
    symbols = df["symbol"].unique()
    hdbg.dassert_eq(len(symbols), 1)
    symbol = symbols[0]
    #
    asset_ids = df["asset_id"].unique()
    hdbg.dassert_eq(len(asset_ids), 1)
    asset_id = asset_ids[0]
    # Accumulate buys and sells.
    buy_count = df["buy_count"].sum()
    sell_count = df["sell_count"].sum()
    # Accumulate taker/maker counts.
    taker_count = df["taker_count"].sum()
    maker_count = df["maker_count"].sum()
    # Accumulate buys and sells volumes.
    buy_volume = df["buy_volume"].sum()
    sell_volume = df["sell_volume"].sum()
    # Accumulate taker/maker volumes.
    taker_volume = df["taker_volume"].sum()
    maker_volume = df["maker_volume"].sum()
    # Accumulate buys and sells notional.
    buy_notional = df["buy_notional"].sum()
    sell_notional = df["sell_notional"].sum()
    # Accumulate taker/maker notional.
    taker_notional = df["taker_notional"].sum()
    maker_notional = df["maker_notional"].sum()
    # Accumulate share counts and costs.
    amount = df["amount"].sum()
    cost = df["cost"].sum()
    transaction_cost = df["transaction_cost"].sum()
    realized_pnl = df["realized_pnl"].sum()
    # Compute average price.
    price = cost / amount
    # Package the results and return.
    aggregated = pd.Series(
        {
            "first_timestamp": first_timestamp,
            "last_timestamp": last_timestamp,
            "first_datetime": first_datetime,
            "last_datetime": last_datetime,
            "symbol": symbol,
            "asset_id": asset_id,
            "buy_count": buy_count,
            "sell_count": sell_count,
            "taker_count": taker_count,
            "maker_count": maker_count,
            "buy_volume": buy_volume,
            "sell_volume": sell_volume,
            "taker_volume": taker_volume,
            "maker_volume": maker_volume,
            "buy_notional": buy_notional,
            "sell_notional": sell_notional,
            "taker_notional": taker_notional,
            "maker_notional": maker_notional,
            "price": price,
            "amount": amount,
            "cost": cost,
            "transaction_cost": transaction_cost,
            "realized_pnl": realized_pnl,
        }
    )
    return aggregated
