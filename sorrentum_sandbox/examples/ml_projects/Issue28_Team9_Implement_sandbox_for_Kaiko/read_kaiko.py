import kaiko
import numpy as np
import pandas as pd
import download_kaiko as dk

# Setting a client with API key
api_key = "1d16b71fdfa506550a8a9cc5faa36fa4"
kc = kaiko.KaikoClient(api_key=api_key)

# Test : TickTrade data
def ticktrade():
    ticktrade = kaiko.Trades(
        data_version="v1",
        exchange="cbse",
        instrument="btc-usd",
        start_time="2022-1-1 0:00",
        end_time="2022-1-1 1:00",
        client=kc
    )
    return ticktrade

def book_snapshot():
    book_snapshot = kaiko.OrderBookSnapshots(
        exchange="cbse",
        instrument="btc-usd",
        start_time="2023-3-6",
        end_time="2023-3-7",
        client=kc
    )
    return book_snapshot
downloader = dk.KaikoDownloader()
result = downloader.download(
    start_timestamp="2022-1-1 0:00",
    end_timestamp="2022-1-1 1:00"
)
print(result.get_data())