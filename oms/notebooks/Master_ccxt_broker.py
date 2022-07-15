# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
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
# # Description

# %% [markdown]
# This notebook contains examples of CCXT DB Broker functionality.

# %%
# %load_ext autoreload
# %autoreload 2
import logging

import pandas as pd

import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hprint as hprint
import helpers.hsql as hsql
import im_v2.common.data.client as icdc
import im_v2.im_lib_tasks as imvimlita
import market_data as mdata
import oms.ccxt_broker as occxbrok
import oms.order as omorder
import oms.order_example as oordexam

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %% [markdown]
# ## DB

# %% [markdown]
# Establish a DB connection and example market data.
#
# `MarketData` object is required for instantiation of all brokers, since it is used in `OrderProcessor` down the pipeline.

# %% [markdown]
# ### Connection

# %%
# Get environment variables with login info.
env_file = imvimlita.get_db_env_path("dev")
# Get login info.
connection_params = hsql.get_connection_info_from_env_file(env_file)
# Login.
connection = hsql.get_connection(*connection_params)

# %% [markdown]
# ### Market data

# %%
hsql.remove_table(connection, "example2_marketdata")
im_client = icdc.get_mock_realtime_client(connection)
market_data = mdata.get_RealtimeMarketData_example1(im_client)

# %%
# Load an example of CCXT order.
order = oordexam.get_order_example4()
print(omorder.orders_to_string([order]))

# %% [markdown]
# ## Demonstration of main Broker methods

# %% [markdown]
# ### `init`

# %%
exchange_id = "binance"
universe_version = "v5"
contract_type = "futures"
mode = "test"

# %%
# Initialize CCXT broker with example market data connected to DB.
broker = occxbrok.CcxtBroker(
    exchange_id,
    universe_version,
    mode,
    contract_type,
    market_data=market_data,
    strategy_id="SAU1",
)

# %% [markdown]
# ### `submit_orders`

# %%
orders = [order, order]
# Submitting orders to exchange and getting the
order_resps = await broker._submit_orders(orders, pd.Timestamp.utcnow())

# %%
order_resps

# %% [markdown]
# ### `get_fills`

# %%
fills = broker.get_fills(order_resps)

# %%
fills

# %%
fills[0].to_dict()

# %% [markdown]
# #### Comment

# %% [markdown]
# - The Fills are currently filtered by last execution ts; since the orders are executed immediately (at least in the sandbox environment), this means that only the latest order in the session is returning a Fill;
# - One way to fight this is to remove the filtering by datetime and instead filter by IDs, i.e. get all our orders from the exchange and quickly filter out those we sent during this session by order ID.
