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
# %%
# # %load_ext autoreload
# # %autoreload 2
# # %matplotlib inline

# %%
import logging

import pandas as pd

import core.explore as cexplore
import helpers.dbg as dbg
import helpers.env as henv
import helpers.git as hgit
import helpers.printing as hprint
import im.ccxt.data.load.loader as cdlloa

# %%
dbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %%
root_dir = "s3://alphamatic-data/data"
ccxt_loader = cdlloa.CcxtLoader(root_dir=root_dir, aws_profile="am")
ccxt_data = ccxt_loader.read_data(exchange_id="binance", currency_pair="BTC/USDT", data_type="OHLCV")
print(ccxt_data.shape[0])
ccxt_data.head(3)

# %%
# Check the timezone info.
ccxt_data["timestamp"].iloc[0]

# %%
# TODO(*): change tz in `CcxtLoader`.
ccxt_data["timestamp"] = ccxt_data["timestamp"].dt.tz_convert("US/Eastern")
ccxt_data["timestamp"].iloc[0]

# %%
ccxt_data = ccxt_data.set_index("timestamp")
ccxt_data.head(3)

# %%
ccxt_data = ccxt_data[["close"]]
ccxt_data.head(3)

# %%
min_date = ccxt_data.index.min()
max_date = ccxt_data.index.max()
one_minute_index = pd.date_range(min_date, max_date, freq="T")
ccxt_data_reindex = ccxt_data.reindex(one_minute_index)
print(ccxt_data_reindex.shape[0])
ccxt_data_reindex.head(3)


# %%
def filter_by_date(df, start_date, end_date):
    filter_start_date = pd.Timestamp(start_date, tz="US/Eastern")
    filter_end_date = pd.Timestamp(end_date, tz="US/Eastern")
    mask = (df.index >= filter_start_date) & (df.index < filter_end_date)
    _LOG.info(
        "Filtering in [%s; %s), selected rows=%s",
        start_date,
        end_date,
        hprint.perc(mask.sum(), df.shape[0]),
    )
    ccxt_data_filtered = ccxt_data_reindex[mask]
    return ccxt_data_filtered

ccxt_data_filtered = filter_by_date(ccxt_data_reindex, "2019-01-01", "2020-01-01")
ccxt_data_filtered.head(3)

# %%
nan_stats_df = cexplore.report_zero_nan_inf_stats(ccxt_data_filtered)
nan_stats_df

# %%
nan_data = ccxt_data_filtered[ccxt_data_filtered["close"].isna()]
nan_data.groupby(nan_data.index.date).apply(lambda x: x.isnull().sum()).sort_values(by="close", ascending=False)

# %%
ccxt_data_filtered["close"].plot()
