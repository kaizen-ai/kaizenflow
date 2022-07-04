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

# %%
import helpers.hs3 as hs3
import helpers.hsql as hsql
import im_v2.common.data.client as icdc
import im_v2.im_lib_tasks as imvimlita
import market_data as mdata
import oms.ccxt_broker as occxbrok
import oms.oms_db as oomsdb
import oms.order_example as oordexam

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
create_all_tables(connection)

# %% [markdown]
# ### Market data

# %%
# Create an example client connected to DB.
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
order = oordexam.get_order_example4()

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
# In the DB.
query = f"SELECT * FROM {oomsdb.SUBMITTED_ORDERS_TABLE_NAME} LIMIT 10"
hsql.execute_query_to_df(connection, query)

# %%
# On the S3 location.
hs3.listdir(
    "s3://cryptokaizen-data-test/ccxt_db_broker_test/",
    "*",
    True,
    False,
    aws_profile="ck",
)

# %%
hs3.from_file(
    "s3://cryptokaizen-data-test/ccxt_db_broker_test/20220704000000/positions.0.20220704_190819.txt",
    aws_profile="ck",
)

# %%
