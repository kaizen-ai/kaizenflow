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
# import helpers.hsecrets as hsecret

# %%
# api_creds = hsecret.get_secret("binance.local.sandbox.1")

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
# ## Received message example
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
def process_stream_data(websocket_manager: bnwam.BinanceWebSocketApiManager) -> None:
    end_time = datetime.now() + timedelta(seconds=10)
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
markets = universe_single = universe[0:1]

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
time.sleep(10)

# %% [markdown]
# ## Received message example

# %% run_control={"marked": true}
data[0]

# %%
