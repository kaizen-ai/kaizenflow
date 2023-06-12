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
import ccxt
import pandas as pd
import unittest.mock as umock
import im_v2.ccxt.data.extract.extractor as imvcdexex

# %%
original_ccxt = imvcdexex.ccxt

# %%
print(original_ccxt)

# %%
# Mock ccxt here
ccxt_patch = umock.patch.object(imvcdexex, "ccxt", spec=ccxt)
ccxt_patch.start()

current_ccxt = imvcdexex.ccxt

# %%
print(current_ccxt)

# %%
exchange_class = imvcdexex.CcxtExtractor("binanceus", "spot")
start_timestamp = pd.Timestamp("2022-02-24T00:00:00Z")
end_timestamp = pd.Timestamp("2022-02-25T00:00:00Z")
ohlcv_data = exchange_class._download_ohlcv(
                exchange_id="binanceus",
                currency_pair="BTC/USDT",
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
                bar_per_iteration=500,
            )

# %%
ohlcv_data

# %%
