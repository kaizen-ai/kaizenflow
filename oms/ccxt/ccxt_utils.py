"""
Import as:

import oms.ccxt.ccxt_utils as occccuti
"""
import logging
import os
from typing import Any, Dict, List, Tuple, Union

import numpy as np
import pandas as pd

import helpers.hdbg as hdbg
import helpers.hgit as hgit
import helpers.hio as hio
import helpers.hprint as hprint
import oms.order as omorder

_LOG = logging.getLogger(__name__)


def roll_up_child_order_fills_into_parent(
    parent_order: omorder.Order, ccxt_child_order_fills: List[Dict[str, Any]]
) -> Tuple[float, float]:
    """
    Aggregate fill amount and price from child orders into an equivalent parent
    order.

    Fill amount is calculated as sum of fills for all orders.
    Price is calculated as VWAP for each passed child order, based on their fill
    amount.

    :param parent_order: parent order to get fill amount and price for
    :param ccxt_child_order_fills: CCXT order structures for child orders
    :return: fill amount, fill price
    """
    # Get the total fill from all child orders.
    child_order_fill_amounts = []
    for child_order_fill in ccxt_child_order_fills:
        fill_amount = float(child_order_fill["filled"])
        child_order_fill_amounts.append(fill_amount)
    # Roll up information from child fills into an equivalent parent order.
    parent_order_total_fill_num_shares = sum(child_order_fill_amounts)
    if parent_order_total_fill_num_shares == 0:
        # If all no amount is filled, average price is not applicable.
        _LOG.debug(
            "No amount filled for order %s. Skipping.", parent_order.order_id
        )
        parent_order_fill_price = np.nan
    else:
        # Calculate fill price as VWAP of all child order prices.
        # The cost here is the fill price and not the set limit price:
        # https://docs.ccxt.com/#/?id=notes-on-precision-and-limits
        child_order_fill_prices = [
            child_order_fill["cost"]
            for child_order_fill in ccxt_child_order_fills
        ]
        parent_order_fill_price = (
            sum(child_order_fill_prices) / parent_order_total_fill_num_shares
        )
        # Round to the 4 decimals as the price is rounded at exchanges.
        # TODO(Danya): We need to set up this rounding programmatically,
        # if we use a different quote currency than USDT/BUSD.
        parent_order_fill_price = np.round(parent_order_fill_price, 4)
        # Convert to float if the resulting price is an integer.
        # Portfolio downstream only accepts float dtype.
        if isinstance(parent_order_fill_price, int):
            parent_order_fill_price = float(parent_order_fill_price)
        hdbg.dassert_type_in(parent_order_fill_price, [float, np.float64])
    return parent_order_total_fill_num_shares, parent_order_fill_price


# #############################################################################
# Limit price calculation
# #############################################################################


def compare_latest_and_average_price(
    bid_ask_price_data: pd.DataFrame, max_deviation: float
) -> Tuple[Dict[str, float], float, float]:
    """
    Retrieve and compare latest and average bid/ask prices.

    If the latest price deviates from the average price by more than
    `max_deviation`, the average price is used as reference to calculate
    the limit price downstream; otherwise, the latest price is used.

    :param bid_ask_price_data: bid/ask prices for a certain asset
    :param max_deviation: threshold to compare the average and latest prices,
        given as a value in range (0, 1) to express a percentage difference
    :return:
        - information about the latest and average bid/ask prices
        - reference bid price
        - reference ask price
    """
    # Verify that the maximum deviation is in (0, 1).
    hdbg.dassert_lgt(
        0, max_deviation, 1, lower_bound_closed=False, upper_bound_closed=False
    )
    # Get the latest bid/ask price.
    bid_price_latest, ask_price_latest = bid_ask_price_data.iloc[-1]
    # Get the average bid/ask prices over the period.
    bid_price_mean, ask_price_mean = bid_ask_price_data.mean()
    #
    out_data = {
        "latest_bid_price": bid_price_latest,
        "latest_ask_price": ask_price_latest,
        "bid_price_mean": bid_price_mean,
        "ask_price_mean": ask_price_mean,
    }
    # Compare the prices using the `max_deviation` threshold.
    # Determine reference bid price.
    if (
        abs((bid_price_latest - bid_price_mean) / bid_price_latest)
        > max_deviation
    ):
        bid_price = bid_price_mean
        out_data["used_bid_price"] = "bid_price_mean"
        _LOG.debug(
            "Latest price differs more than %s percent from average price: %s",
            max_deviation * 100,
            hprint.to_str("bid_price_latest bid_price_mean"),
        )
    else:
        bid_price = bid_price_latest
        out_data["used_bid_price"] = "latest_bid_price"
    # Determine reference ask price.
    if (
        abs((ask_price_latest - ask_price_mean) / ask_price_latest)
        > max_deviation
    ):
        ask_price = ask_price_mean
        out_data["used_ask_price"] = "ask_price_mean"
        _LOG.debug(
            "Latest price differs more than %s percent from average price: %s",
            max_deviation * 100,
            hprint.to_str("ask_price_latest ask_price_mean"),
        )
    else:
        ask_price = ask_price_latest
        out_data["used_ask_price"] = "latest_ask_price"
    return out_data, bid_price, ask_price


def calculate_limit_price(
    bid_ask_data: pd.DataFrame,
    asset_id: int,
    side: str,
    passivity_factor: float,
    max_deviation: float = 0.01,
) -> Dict[str, float]:
    """
    Calculate limit price based on recent bid / ask data.

    The limit price is adjusted using the passivity factor.
    The function returns a dict with:
    - calculated limit price;
    - latest bid/ask prices;
    - mean bid/ask prices in the given data;
    - passivity factor;
    - whether limit bid/ask prices are calculated using latest
        price or average price

    :param bid_ask_data: recent bid / ask data, e.g.:
                                        asset_id           end_download_timestamp              knowledge_timestamp  bid_size_l1  bid_size_l2 ...  bid_price_l1  bid_price_l2 ...  ask_size_l1  ask_size_l2 ... ask_price_l1  ask_price_l2
    end_timestamp
    2023-05-09 06:49:49.794000-04:00  2683705052 2023-05-09 10:49:50.053927+00:00 2023-05-09 10:49:50.516534+00:00      12902.0      16730.0 ...    0.8892        0.8891   ...      1506.0       8793.0        ...  0.8893        0.8894
    2023-05-09 06:49:49.804000-04:00  8717633868 2023-05-09 10:49:50.053196+00:00 2023-05-09 10:49:50.516534+00:00        204.0        265.0 ...   15.3810       15.3800   ...      1172.0        219.0         ...  15.3820       15.3830
    :param asset_id: asset for which to get limit price
    :param side: "buy" or "sell"
    :param passivity_factor: value in [0,1] to calculate limit price
    :param max_deviation: see `compare_latest_and_average_price()`
    :return: limit price and price data
    """
    # Verify that the passivity factor is in [0,1].
    hdbg.dassert_lgt(
        0, passivity_factor, 1, lower_bound_closed=True, upper_bound_closed=True
    )
    # Declare columns with bid/ask prices and verify that they exist and contain
    # numeric data (i.e. data that can be converted to float).
    bid_price_col = "bid_price_l1"
    ask_price_col = "ask_price_l1"
    for col in [bid_price_col, ask_price_col]:
        hdbg.dassert_in(col, bid_ask_data.columns)
        hdbg.dassert_type_is(float(bid_ask_data.iloc[0][col]), float)
    # Verify that there is data for the specified asset.
    hdbg.dassert_in("asset_id", bid_ask_data.columns)
    hdbg.dassert_lt(0, len(bid_ask_data[bid_ask_data["asset_id"] == asset_id]))
    # Initialize price dictionary.
    price_dict: Dict[str, Any] = {}
    price_dict["passivity_factor"] = passivity_factor
    # Select bid/ask price data by requested asset id.
    bid_ask_data = bid_ask_data.loc[bid_ask_data["asset_id"] == asset_id]
    bid_ask_price_data = bid_ask_data[[bid_price_col, ask_price_col]]
    # Retrieve and compare latest and average prices.
    # The difference between these prices determines the reference
    # bid and ask prices to use further.
    (
        latest_avg_prices_info,
        bid_price,
        ask_price,
    ) = compare_latest_and_average_price(bid_ask_price_data, max_deviation)
    price_dict.update(latest_avg_prices_info)
    # Adjust limit price based on passivity factor.
    # - limit_price in [bid,ask];
    # - if side == "buy":
    #   - passivity == 1 -> limit_price = bid
    # - limit_price in [bid,ask];
    # - if side == "buy":
    #   - passivity == 1 -> limit_price = bid
    #   - passivity == 0 -> limit_price = ask
    # - if side == "sell":
    #   - passivity == 1 -> limit_price = ask
    #   - passivity == 0 -> limit_price = bid
    # - passivity_factor == 0.5 is a midpoint in both cases.
    if side == "buy":
        limit_price = (bid_price * passivity_factor) + (
            ask_price * (1 - passivity_factor)
        )
    elif side == "sell":
        limit_price = (ask_price * passivity_factor) + (
            bid_price * (1 - passivity_factor)
        )
    else:
        raise ValueError(f"Invalid side='{side}'")
    price_dict["limit_price"] = limit_price
    return price_dict


# #############################################################################


def subset_market_info(
    market_info: Dict[int, Dict[str, Union[float, int]]], info_type: str
) -> Dict[int, Union[float, int]]:
    """
    Return only the relevant information from market info, e.g., info about
    precision.
    """
    # It is assumed that every asset has the same info type structure.
    available_info = list(market_info.values())[0].keys()
    hdbg.dassert_in(info_type, available_info)
    market_info_keys = list(market_info.keys())
    _LOG.debug("market_info keys=%s", market_info_keys)
    asset_ids_to_decimals = {
        key: market_info[key][info_type] for key in market_info_keys
    }
    return asset_ids_to_decimals


def load_market_data_info() -> Dict[int, Dict[str, Union[float, int]]]:
    """
    Load pre-saved market info.

    The data looks like:
    {"6051632686":
        {"min_amount": 1.0, "min_cost": 10.0, "amount_precision": 3},
     ...
    """
    file_path = os.path.join(
        hgit.get_amp_abs_path(),
        "oms/ccxt/test/outcomes/TestSaveBrokerData1.test_save_market_info1/input/broker_data.json",
    )
    market_info = hio.from_json(file_path)
    # Convert to int, because asset ids are strings.
    market_info = {int(k): v for k, v in market_info.items()}
    return market_info
