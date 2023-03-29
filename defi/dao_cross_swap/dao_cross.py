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

# %%
# %load_ext autoreload
# %autoreload 2

import copy
import heapq
import logging

import numpy as np
import pandas as pd

from tqdm.autonotebook import tqdm
from typing import List

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hprint as hprint

# %%
hdbg.init_logger(verbosity=logging.DEBUG)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()


# %% [markdown]
# # Order class

# %%
# TODO(gp): Consider reordering matching the white paper order.
class Order:
    
    def __init__(
        self,
        base_token: str,
        quote_token: str,
        action: str,
        quantity: float,
        limit_price: float,
        deposit_address,
        wallet_address,
    ) -> None:
        self.base_token = base_token
        self.quote_token = quote_token 
        hdbg.dassert_in(action, ["buy", "sell"])
        self.action = action
        self.quantity = quantity
        # Replace NaN with signed Inf depending upon `action`.
        # This helps with `Order` comparisons (`lt` and `gt`).
        if np.isnan(limit_price):
            if action == "buy":
                self.limit_price = np.inf
            elif action == "sell":
                self.limit_price = -np.inf
            else:
                raise ValueError
        else:
            self.limit_price = limit_price
        self.timestamp = hdateti.get_current_time(tz="UTC")
        self.deposit_address = deposit_address
        self.wallet_address = wallet_address
        
    def __repr__(self):
        return str(self)
        
    def __str__(self):
        ret = "base_token=%s quote_token=%s action=%s quantity=%s limit_price=%s timestamp=%s deposit_address=%s" % (self.base_token, self.quote_token, self.action, self.quantity, self.limit_price, self.timestamp, self.deposit_address)
        return ret
    
    # Because we use a min-heap, taking precedence in the order queue is given
    # "lower priority" in the internal heap.
    def __lt__(self, other):
        return self._takes_precedence(other)

    def __gt__(self, other):
        return not self._takes_precedence(other)
    
    def _takes_precedence(self, other):
        # Prioritize according to quantity/price/timestamp.
        takes_precedence = False
        if self.quantity > other.quantity:
            takes_precedence = True
        elif self.limit_price > other.limit_price:
            takes_precedence = True
        elif self.timestamp > other.timestamp:
            takes_precedence = True
        return takes_precedence


# %%
buy_order1 = Order("ETH", "BTC", "buy", 10, 0.1, 1, 1)
buy_order2 = Order("ETH", "BTC", "buy", 9, 0.15, 2, 2)
buy_order3 = Order("ETH", "BTC", "buy", 10, 0.15, 3, 3)
buy_orders = [buy_order1, buy_order2, buy_order3]
display(buy_orders)

# %%
sell_order1 = Order("ETH", "BTC", "sell", 11, 0.1, -1, -1)
sell_order2 = Order("ETH", "BTC", "sell", 10, 0.15, -2, -2)
sell_order3 = Order("ETH", "BTC", "sell", 9, 0.15, -3, -3)
sell_orders = [sell_order1, sell_order2, sell_order3]
display(sell_orders)


# %%
def get_random_order(seed=None):
    if seed is not None:
        np.random.seed(seed)
    base_token = "ETH"
    quote_token = "BTC"
    # Generate random buy/sells.
    action = "buy" if np.random.random() < 0.5 else "sell"
    # Generate random quantities.
    quantity = np.random.randint(1, 10)
    # Do not impose a limit price.
    limit_price = np.nan
    # Create a random wallet address.
    deposit_address = np.random.randint(-3, 3)
    # Prevent self-crossing (in a crude way).
    if action == "buy":
        deposit_address = abs(deposit_address)
    elif action == "sell":
        deposit_address = -abs(deposit_address)
    # Make wallet address and deposit address the same.
    wallet_address = deposit_address
    # Build order.
    order = Order(base_token, quote_token, action, quantity, limit_price, deposit_address, wallet_address)
    return order


# %%
get_random_order()


# %% [markdown]
# # Order matching

# %%
def is_active_order(order):
    if order is None:
        return False
    if not order.quantity > 0:
        return False
    return True


def get_transfer_df(transfers: List):
    if transfers:
        transfer_df = pd.concat(transfers, axis=1).T
    else:
        transfer_df = pd.DataFrame(
            columns=["token", "amount", "from", "to"]
        )
    return transfer_df


def match_orders(orders: List[Order], clearing_price) -> pd.DataFrame:
    # Build buy and sell heaps.
    buy_heap = []
    sell_heap = []
    for order in orders:
        if order.action == "buy":
            if order.limit_price >= clearing_price:
                heapq.heappush(buy_heap, order)
            else:
                _LOG.debug("Order not eligible for matching due to limit")
        elif order.action == "sell":
            if order.limit_price <= clearing_price:
                heapq.heappush(sell_heap, order)
            else:
                _LOG.debug("Order not eligible for matching due to limit")
        else:
            raise ValueError
    _LOG.debug("buy_heap=%s", buy_heap)
    _LOG.debug("sell_heap=%s", sell_heap)
    # Perform matching.
    transfers = []
    # TODO: Make sure orders are compatible and valid.
    # Successively compare buy_heap top with sell_heap top, matching volume until
    # zero or queues empty.
    buy_order = None
    sell_order = None
    while (buy_heap or is_active_order(buy_order)) and (sell_heap or is_active_order(sell_order)):
        if not buy_order or buy_order.quantity == 0:
            # Make a copy so that `match_orders()` does not alter state (and is idempotent).
            buy_order = copy.copy(buy_heap.pop())
        if not sell_order or sell_order.quantity == 0:
            sell_order = copy.copy(sell_heap.pop())
        quantity = min(buy_order.quantity, sell_order.quantity)
        # TODO(gp): Encode as dict and push the representation in get_transfer_df.
        transfer1 = pd.Series({
            "token": buy_order.base_token,
            "amount": quantity,
            "from": sell_order.wallet_address,
            "to": buy_order.deposit_address
        })
        transfers.append(transfer1)
        transfer2 = pd.Series({
            "token": buy_order.quote_token,
            "amount": quantity * clearing_price,
            "from": buy_order.wallet_address,
            "to": sell_order.deposit_address,
        })
        transfers.append(transfer2)
        buy_order.quantity -= quantity
        sell_order.quantity -= quantity
    transfer_df = get_transfer_df(transfers)
    return transfer_df


# %% [markdown]
# ## Example 1

# %%
clearing_price = 0.14
transfers_df = match_orders(buy_orders + sell_orders, clearing_price)

# %%
eth_transfers = transfers_df[transfers_df["token"] == "ETH"]
display(eth_transfers)

# %%
# Transferred ETH (in ETH).
eth_transfers["amount"].sum()

# %%
btc_transfers = transfers_df[transfers_df["token"] == "BTC"]
display(btc_transfers)

# %%
# Transferred BTC (in BTC).
btc_transfers["amount"].sum()

# %%
# BTC transferred divided by the ETH/BTC price (quoted in BTC per ETH)
# returns amount of transferred ETH.
btc_transfers["amount"].sum() / clearing_price

# %% [markdown]
# ## Example 2

# %%
orders = []
for idx in range(0, 10):
    order = get_random_order(idx)
    orders.append(order)

# %%
orders

# %%
transfers_df_2 = match_orders(orders, 0.1)
display(transfers_df_2)

# %%
