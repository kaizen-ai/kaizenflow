# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.13.0
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %%
# %load_ext autoreload
# %autoreload 2

import logging

import numpy as np
import pandas as pd

import core.config as cconfig
import core.dataflow.price_interface as cdtfprint
import helpers.dbg as hdbg
import helpers.printing as hprint
import oms.portfolio as omportfo
import oms.test.test_portfolio as ottport

# %%
# hdbg.init_logger(verbosity=logging.INFO)
hdbg.init_logger(verbosity=logging.DEBUG)

_LOG = logging.getLogger(__name__)

# _LOG.info("%s", env.get_system_signature()[0])

hprint.config_notebook()

# %% [markdown]
# # Section 1

# %%
event_loop = None
price_interface = ottport.get_replayed_time_price_interface(event_loop)

# %%
initial_timestamp = pd.Timestamp("2000-01-01 09:35:00-05:00")
portfolio = ottport.get_portfolio_example1(price_interface, initial_timestamp)

# %%
str(portfolio)

# %%
portfolio.holdings

# %%
portfolio.get_holdings(initial_timestamp, asset_id=-1, exclude_cash=True)

# %%
portfolio.get_holdings_as_scalar(pd.Timestamp("2000-01-01 09:35:00-05:00"), 0)

# %%
srs = portfolio.get_characteristics(initial_timestamp)
display(srs)

# %%
portfolio.orders

# %%
portfolio.get_net_wealth(initial_timestamp)

# %%
