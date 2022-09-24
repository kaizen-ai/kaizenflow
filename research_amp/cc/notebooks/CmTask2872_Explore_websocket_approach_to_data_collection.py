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
# # Python-Binance lib approach

# %% [markdown]
# As a first approach we will try python-binance lib. We need to install it, since it is not currently a part of dev image

# %%
import sys

# !sudo {sys.executable} -m pip install python-binance

# %%
import asyncio
import binance as bn
from datetime import datetime, timedelta

# %% [markdown]
# ## Single symbol demo

# %%
msgs = []
def process_websocket_msg(msg):
    msgs.append(msg)


# %%
async def start_order_book_socket():
    # client = await bn.AsyncClient.create(api_key=api_creds["apiKey"], api_secret=api_creds["secret"])
    # For this data we don't even need API key.
    client = await bn.AsyncClient.create()
    # Setting custom timeout when executing with this approach doesn't work.
    # i.e. the socket simply continues to stream.
    socket_manager = bn.BinanceSocketManager(client, user_timeout=10)
    # start the socket
    # The library is not a 100% match against the API
    # For example we don't have the update speed argument
    # available.
    order_book_socket = socket_manager.futures_depth_socket('BTCUSDT',
                                                           futures_type=bn.enums.FuturesType.USD_M,
                                                           depth=bn.BinanceSocketManager.WEBSOCKET_DEPTH_5)
    end_time = datetime.now() + timedelta(seconds=10)
    async with order_book_socket as obsm:
        while datetime.now() < end_time:
            msg = await obsm.recv()
            process_websocket_msg(msg)

    await client.close_connection()

# %%
# For some reason running the below code ends with:
# RuntimeError: This event loop is already running
#loop = asyncio.get_event_loop()
#loop.run_until_complete(start_order_book_socket())
# We need to run using
await start_order_book_socket()

# %% [markdown]
# ### Received message example
# - From binance forum https://dev.binance.vision/t/meaning-of-event-time-transaction-time-fields/5449
# - The transaction time T records the time that the data (e.g. account, order related) got updated and the event time E represents the time a certain data was pushed out from the server
# - "When calculating server-to-client latency, I would suggest to consider using event time since the transaction time can be viewed as the internal operation time at the server side."
#  - **Should we be calculating the latency?**

# %%
msgs[0]

# %% [markdown]
# # Unicorn-binance-websocket lib approach

# %%
import sys

# !sudo {sys.executable} -m pip install unicorn-binance-websocket-api

# %%
import unicorn_binance_websocket_api.manager as bnwam
import im_v2.common.universe.universe as imvcounun
from datetime import datetime, timedelta
import time
import threading
import json
import pandas as pd

# %%
universe = imvcounun.get_vendor_universe("CCXT", "download", version="v7")
universe = universe['binance']
# convert to websocket format
universe = list(map(lambda x: x.replace("_", "").lower(), universe))

# %%
bn_websocket_manager = bnwam.BinanceWebSocketApiManager(exchange="binance.com-futures")

# %% [markdown]
# With regards to airflow we can decide if we want to implement our own timecounter
# or simply rely on the fact that a single websocket connection lasts 24h.
# Having our counter will probably make it easier to sync individual downloaders scheduled one after another

# %%
# Setup callback function
data = []
run_for = 10 #seconds
def process_stream_data(websocket_manager: bnwam.BinanceWebSocketApiManager) -> None:
    end_time = datetime.now() + timedelta(seconds=run_for)
    while datetime.now() < end_time:
        if websocket_manager.is_manager_stopping():
            break
        oldest_stream_data_from_stream_buffer = websocket_manager.pop_stream_data_from_stream_buffer()
        if oldest_stream_data_from_stream_buffer is False:
            time.sleep(0.01)
        else:
            data.append(oldest_stream_data_from_stream_buffer)
    websocket_manager.stop_manager_with_all_streams()


# %% [markdown]
# ## Single symbol demo

# %%
markets = universe[0:1]

# %%
freq = 500 # miliseconds
depth = 5
channels = [f"depth{depth}@{freq}ms"]
# There is a limit of maximum 1024 subscriptions calculated as no. of streams * no. of markets
# output="dict" has to be set, otherwise we get raw data as string
bn_websocket_manager.create_stream(channels, markets, output="dict")

# %%
worker_thread = threading.Thread(target=process_stream_data, args=(bn_websocket_manager,))
worker_thread.start()
# wait until the worker finishes
time.sleep(run_for)

# %% [markdown]
# ### Received message example

# %% run_control={"marked": true}
data[0]

# %% [markdown]
# ## Full universe demo

# %%
freq = 500 # miliseconds
depth = 5
channels = [f"depth{depth}@{freq}ms"]
# Reset the data list
data = []
# Set running time
run_for = 10 # seconds
markets = universe
# There is a limit of maximum 1024 subscriptions calculated as no. of streams * no. of markets
# output="dict" has to be set, otherwise we get raw data as string
bn_websocket_manager = bnwam.BinanceWebSocketApiManager(exchange="binance.com-futures")
bn_websocket_manager.create_stream(channels, markets, output="dict")

# %%
worker_thread = threading.Thread(target=process_stream_data, args=(bn_websocket_manager,))

# %% [markdown]
# ### Monitoring example
#
# We can get monitoring of the websocket status and stats through python or also webserver based (not available in the curreny docker jupyter setup)
# https://github.com/LUCIT-Systems-and-Development/unicorn-binance-websocket-api/wiki/UNICORN-Monitoring-API-Service

# %%
worker_thread.start()
for _ in range(run_for):
    print("Plain monitoring status:")
    print(bn_websocket_manager.get_monitoring_status_plain())
    print("-----")
    print("Icinga monitoring status")
    print(bn_websocket_manager.get_monitoring_status_icinga())
    print("-----")
    time.sleep(1)

# %% [markdown]
# ### Received data transformation
#
# First received message is sometimes '{'id': 1, 'result': None}' reason is unclear at the moment

# %%
# Clean noise
print(len(data))
data = list(filter(lambda x: "data" in x, data))
print(len(data))

# %%
df_full = pd.concat(list(map(pd.json_normalize, data)))
df_full = df_full.explode(["data.a", "data.b"])
df_full[["bid_price", "bid_size"]] = pd.DataFrame(
df_full["data.b"].to_list(), index=df_full.index)
df_full[["ask_price", "ask_size"]] = pd.DataFrame(
df_full["data.a"].to_list(), index=df_full.index)
df_full["level"] = df_full.groupby(["data.s", "data.T"])[["data.s", "data.T"]].cumcount().add(1)

# %%
df_full.head()

# %%
df_full['data.s'].value_counts()

# %% [markdown]
# # Saving bid/ask data to the DB: flow proposal

# %%
import helpers.hsql as hsql
import im_v2.im_lib_tasks as imvimlita
import helpers.hdatetime as hdateti
from typing import Dict, List
import helpers.hpandas as hpandas
import helpers.hdbg as hdbg
import helpers.henv as henv
import logging

# %%
hdbg.init_logger(verbosity=logging.ERROR)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

# %%
env_file = imvimlita.get_db_env_path("dev")
connection_params = hsql.get_connection_info_from_env_file(env_file)
db_connection = hsql.get_connection(*connection_params)

# %%
# Mapping of the keys of the message relevant to us
# to our internal naming convention.
# end_download_timestamp is added by us upon receival of the data
relevant_column_mapping = {"T": "timestamp", 
                           "s": "currency_pair", 
                           "b": "bids", 
                           "a": "asks"}
def format_websocket_message(msg: Dict) -> Dict:
    """
    TODO(Juraj): add example of before and after
    """
    msg = msg["data"]
    return { relevant_column_mapping[k]:msg[k] for k in relevant_column_mapping.keys() }

def transform_dict_data_to_df(raw_dict_data: List[Dict]) -> pd.DataFrame:
    df_full = pd.DataFrame(raw_dict_data)
    df_full = df_full.explode(["asks", "bids"])
    df_full[["bid_price", "bid_size"]] = pd.DataFrame(
    df_full["bids"].to_list(), index=df_full.index)
    df_full[["ask_price", "ask_size"]] = pd.DataFrame(
    df_full["asks"].to_list(), index=df_full.index)
    groupby_cols = ["currency_pair", "timestamp"]
    df_full["level"] = df_full.groupby(groupby_cols)[groupby_cols].cumcount().add(1)
    return df_full[["timestamp", "bid_size", "bid_price", "ask_size", 
                    "ask_price", "currency_pair", "level", "end_download_timestamp"]]

def insert_buffered_data_into_db(db_buffer: List[Dict], exchange_id: str, db_connection, db_table: str) -> None:
    df = transform_dict_data_to_df(db_buffer)
    df["exchange_id"] = exchange_id
    df["knowledge_timestamp"] = hdateti.get_current_time("UTC")
    hsql.execute_insert_query(
        connection=db_connection,
        obj=df,
        table_name=db_table,
    )


# %%
# Setup callback function
run_for = 20 # seconds
max_db_buffer_size = 100
db_table = "ccxt_bid_ask_futures_test"
df_example = None
def buffer_and_save_stream_data(websocket_manager: bnwam.BinanceWebSocketApiManager) -> None:
    end_time = datetime.now() + timedelta(seconds=run_for)
    # TO avoid overhead of inserting few data points at a time we can buffer to a larger size
    db_buffer = []
    while datetime.now() < end_time:
        if websocket_manager.is_manager_stopping():
            break
        oldest_stream_data = websocket_manager.pop_stream_data_from_stream_buffer()
        # If the dict is the above mentioned: '{'id': 1, 'result': None}'
        if oldest_stream_data is False or "data" not in oldest_stream_data:
            time.sleep(0.01)
        # TODO(Juraj): handle error messages
        else:
            end_download_timestamp = hdateti.get_current_time("UTC")
            oldest_stream_data = format_websocket_message(oldest_stream_data)
            oldest_stream_data["end_download_timestamp"] = end_download_timestamp
            db_buffer.append(oldest_stream_data)
            if len(db_buffer) >= max_db_buffer_size:
                insert_buffered_data_into_db(db_buffer, "binance", db_connection, db_table)
                # Empty the buffer for next batch.
                db_buffer = []
    # Insert also the last buffer content.
    insert_buffered_data_into_db(db_buffer, "binance", db_connection, db_table)
    websocket_manager.stop_manager_with_all_streams()


# %%
freq = 500 # miliseconds
depth = 5
channels = [f"depth{depth}@{freq}ms"]
# Reset the data list
data = []
# Set running time
run_for = 5 # seconds
markets = universe
# There is a limit of maximum 1024 subscriptions calculated as no. of streams * no. of markets
# output="dict" has to be set, otherwise we get raw data as string
bn_websocket_manager = bnwam.BinanceWebSocketApiManager(exchange="binance.com-futures")
bn_websocket_manager.create_stream(channels, markets, output="dict")

# %%
worker_thread = threading.Thread(target=buffer_and_save_stream_data, args=(bn_websocket_manager,))
worker_thread.start()
for _ in range(run_for):
    print("Plain monitoring status:")
    print(bn_websocket_manager.get_monitoring_status_plain())
    print("-----")
    time.sleep(1)

# %% [markdown]
# # Error Handling

# %% [markdown]
# # OHLCV Example

# %% [markdown]
# # Saving OHLCV data to DB: flow proposal

# %%
