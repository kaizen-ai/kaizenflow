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

# %%
import logging
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hprint as hprint
import im_v2.ccxt.data.client as icdcl

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %%
resample_1min = True
root_dir = "s3://cryptokaizen-data/reorg/historical.manual.pq/"
partition = "by_year_month"
universe_version = "v3"
aws_profile = "ck"
ccxt_historical_client = icdcl.ccxt_clients.CcxtHistoricalPqByTileClient(
    universe_version,
    resample_1min,
    root_dir,
    partition,
    aws_profile=aws_profile,
)

# %%
full_symbol = ["binance::ADA_USDT"]
start_ts = pd.Timestamp("2018-08-17 00:00:00-0000")
end_ts = pd.Timestamp("2018-08-17 00:10:00-0000")
filter_data_mode = "assert"
data = ccxt_historical_client.read_data(
    full_symbol, start_ts, end_ts, columns=None, filter_data_mode=filter_data_mode
)
data.head(3)

# %%
fig, ax = plt.subplots(figsize=(10, 6))
ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
ax.bar(data.index, data['close'])
