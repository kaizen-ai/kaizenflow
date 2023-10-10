"""
Import as:

import oms.broker.ccxt.ccxt_utils as obccccut
"""
import logging
import os
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd

import helpers.hdbg as hdbg
import helpers.hgit as hgit
import helpers.hio as hio
import oms.order.order as oordorde

_LOG = logging.getLogger(__name__)


def roll_up_child_order_fills_into_parent(
    parent_order: oordorde.Order,
    ccxt_child_order_fills: List[Dict[str, Any]],
    *,
    price_quantization: Optional[int] = None,
) -> Tuple[float, float]:
    """
    Aggregate fill amount and price from child orders into an equivalent parent
    order.

    Fill amount is calculated as sum of fills for all orders.
    Price is calculated as VWAP for each passed child order, based on their fill
    amount.

    :param parent_order: parent order to get fill amount and price for
    :param ccxt_child_order_fills: CCXT order structures for child orders
    :param price_quantization: number of decimals to round the price of parent order
    :return: fill amount, fill price
    """
    # Get the total fill from all child orders.
    child_order_signed_fill_amounts = []
    for child_order_fill in ccxt_child_order_fills:
        # Get the unsigned fill amount.
        abs_fill_amount = float(child_order_fill["filled"])
        # Change the sign for 'sell' orders.
        side = child_order_fill["side"]
        if side == "buy":
            signed_fill_amount = abs_fill_amount
        elif side == "sell":
            signed_fill_amount = -abs_fill_amount
        else:
            raise ValueError(f"Unrecognized order `side` {side}")
        child_order_signed_fill_amounts.append(signed_fill_amount)
    signed_fill_amounts = pd.Series(child_order_signed_fill_amounts)
    # Verify that aggregated child orders have the same side.
    # All child orders for the given parent should be only 'buy' or 'sell'.
    hdbg.dassert_eq(
        signed_fill_amounts.abs().sum(),
        abs(signed_fill_amounts.sum()),
        "Both `buys` and `sells` detected in child order rollup.",
    )
    # Roll up information from child fills into an equivalent parent order.
    parent_order_signed_fill_num_shares = signed_fill_amounts.sum()
    if parent_order_signed_fill_num_shares == 0:
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
        parent_order_fill_price = sum(child_order_fill_prices) / abs(
            parent_order_signed_fill_num_shares
        )
        # Round the price to decimal places provided in price_quantization,
        # if provided.
        if price_quantization is not None:
            hdbg.dassert_type_is(price_quantization, int)
            parent_order_fill_price = np.round(
                parent_order_fill_price, price_quantization
            )
        # Convert to float if the resulting price is an integer.
        # Portfolio downstream only accepts float dtype.
        if isinstance(parent_order_fill_price, int):
            parent_order_fill_price = float(parent_order_fill_price)
        hdbg.dassert_type_in(parent_order_fill_price, [float, np.float64])
    return parent_order_signed_fill_num_shares, parent_order_fill_price


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
        "oms/broker/ccxt/test/outcomes/TestSaveBrokerData1.test_save_market_info1/input/broker_data.json",
    )
    market_info = hio.from_json(file_path)
    # Convert to int, because asset ids are strings.
    market_info = {int(k): v for k, v in market_info.items()}
    return market_info
