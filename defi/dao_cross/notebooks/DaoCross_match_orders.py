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

import logging

import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hprint as hprint
import defi.dao_cross.dao_cross as ddcrdacr
import defi.dao_cross.order as ddacrord

# %%
hdbg.init_logger(verbosity=logging.DEBUG)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %%
buy_order1 = ddacrord.Order("ETH", "BTC", "buy", 10, 0.1, 1, 1)
buy_order2 = ddacrord.Order("ETH", "BTC", "buy", 9, 0.15, 2, 2)
buy_order3 = ddacrord.Order("ETH", "BTC", "buy", 10, 0.15, 3, 3)
buy_orders = [buy_order1, buy_order2, buy_order3]
display(buy_orders)

# %%
sell_order1 = ddacrord.Order("ETH", "BTC", "sell", 11, 0.1, -1, -1)
sell_order2 = ddacrord.Order("ETH", "BTC", "sell", 10, 0.15, -2, -2)
sell_order3 = ddacrord.Order("ETH", "BTC", "sell", 9, 0.15, -3, -3)
sell_orders = [sell_order1, sell_order2, sell_order3]
display(sell_orders)

# %%
ddacrord.get_random_order()

# %% [markdown]
# # Order matching

# %% [markdown]
# ## Example 1

# %%
clearing_price = 0.14
transfers_df = ddcrdacr.match_orders(buy_orders + sell_orders, clearing_price)

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
    order = ddacrord.get_random_order(idx)
    orders.append(order)

# %%
orders

# %%
transfers_df_2 = ddcrdacr.match_orders(orders, 0.1)
display(transfers_df_2)

# %%
