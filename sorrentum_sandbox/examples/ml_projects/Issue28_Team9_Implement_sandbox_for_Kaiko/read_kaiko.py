import kaiko
import numpy as np
import pandas as pd

# Setting a client with API key
api_key = "1d16b71fdfa506550a8a9cc5faa36fa4"
kc = kaiko.KaikoClient(api_key=api_key)

# Test : TickTrade data
def ticktrade():
    ticktrade = kaiko.TickTrades(
        exchange="cbse",
        instrument="btc-usd",
        start_time="2022-1-1",
        end_time="2022-1-2",
        client=kc
    )
    return ticktrade

def book_snapshot():
    book_snapshot = kaiko.OrderBookSnapshots(
        exchange="cbse",
        instrument="btc-usd",
        start_time="2023-3-5",
        end_time="2023-3-6",
        client=kc
    )
    return book_snapshot

def book_aggregations():
    book_aggregations = kaiko.OrderBookAggregations(
        exchange="cbse",
        instrument="btc-usd",
        start_time="2023-3-5",
        end_time="2023-3-6",
        client=kc
    )
    return book_aggregations
