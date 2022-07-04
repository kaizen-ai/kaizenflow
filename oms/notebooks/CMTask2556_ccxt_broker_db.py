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
import helpers.hs3 as hs3
import helpers.hsql as hsql
import im_v2.common.data.client as icdc
import im_v2.im_lib_tasks as imvimlita
import market_data as mdata
import oms.ccxt_broker as occxbrok
import oms.oms_db as oomsdb
import oms.order as omorder
import oms.order_example as oordexam

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()


# %% [markdown]
# ## Functions

# %%
def create_all_tables(connection):
    """
    Create submitted and accepted orders tables with default names.
    """
    incremental = False
    oomsdb.create_submitted_orders_table(
        connection, incremental, oomsdb.SUBMITTED_ORDERS_TABLE_NAME
    )
    oomsdb.create_accepted_orders_table(
        connection, incremental, oomsdb.ACCEPTED_ORDERS_TABLE_NAME
    )


# %% [markdown]
# ## DB

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
# ### Creating tables

# %%
# Create tables for submitted and accepted orders.
create_all_tables(connection)

# %% [markdown]
# ### Market data

# %%
# Create an example client connected to DB.
#  Note: Market data format copies CCXT OHLCV data.
hsql.remove_table(connection, "example2_marketdata")
im_client = icdc.get_mock_realtime_client(connection)
market_data = mdata.get_RealtimeMarketData_example1(im_client)

# %% [markdown]
# #### Example of DB data (OHLCV)

# %%
query = "SELECT * FROM example2_marketdata LIMIT 10"
# Execute query and return as pd.DataFrame.
raw_data = hsql.execute_query_to_df(connection, query)
raw_data

# %% [markdown]
# ### Order example

# %%
# Load an example of CCXT order.
order = oordexam.get_order_example4()
print(omorder.orders_to_string([order]))

# %% [markdown]
# ## Demonstration of main Broker methods

# %% [markdown]
# ### Init

# %%
exchange_id = "binance"
universe_version = "v5"
contract_type = "futures"
mode = "test"

# %%
# Initialize CCXT broker with example market data connected to DB.
broker = occxbrok.CcxtDbBroker(
    exchange_id,
    universe_version,
    mode,
    contract_type,
    db_connection=connection,
    submitted_orders_table_name=oomsdb.SUBMITTED_ORDERS_TABLE_NAME,
    accepted_orders_table_name=oomsdb.ACCEPTED_ORDERS_TABLE_NAME,
    strategy_id="SAU1",
    market_data=market_data,
)

# %%
# Submitting orders and uploading them to DB and saving to S3 location.
# Using the internal method to avoid the polling timeout.
await broker._submit_orders([order], pd.Timestamp.utcnow())

# %% [markdown]
# ### Checking submitted orders

# %%
# Example of data inside the submitted orders in the DB.
query = f"SELECT * FROM {oomsdb.SUBMITTED_ORDERS_TABLE_NAME} LIMIT 10"
hsql.execute_query_to_df(connection, query)

# %%
# Example of data in the S3 location.
hs3.listdir(
    "s3://cryptokaizen-data-test/ccxt_db_broker_test/",
    "*",
    True,
    False,
    aws_profile="ck",
)

# %%
hs3.from_file(
    "s3://cryptokaizen-data-test/ccxt_db_broker_test/20220704000000/positions.0.20220704_231752.txt",
    aws_profile="ck",
)

# %% [markdown]
# ## Comment

# %% [markdown]
# - The submitted order data is uploaded into a submitted orders DB table (as in DatabaseBroker abstract class) and saved to S3 location (as in IgBroker)
# - IgBroker saved data only to S3 (w/o the DB upload) and it is not clear what should happen to submitted orders and how precisely they are counted as accepted.
# - One assumption is that there should be an intermediary step to upload the order from DB to the exchange, or the DB itself is hosted by a different entity, e.g. the exchange or an outside broker.
#    - The situation is compounded by a number of references to Java and code not present in the repository.
# - All changes to the broker will be reflected in this notebook.

# %%
