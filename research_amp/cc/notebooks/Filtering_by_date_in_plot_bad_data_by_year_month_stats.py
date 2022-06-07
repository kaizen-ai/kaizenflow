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
# # Imports

# %%
import logging

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


# %% [markdown]
# # Functions

# %%
def plot_by_year_month(
    data: pd.DataFrame,
    threshold: int,
    *,
    start_ts: pd.Timestamp = None,
    end_ts: pd.Timestamp = None,
) -> None:
    df = data.copy()
    # Filtering by date includes start and excludes end timestamps.
    if start_ts is not None:
        df = df[df.index >= start_ts]
    if end_ts is not None:
        df = df[df.index < end_ts]
    # Group data by "full_symbol", "year", "month" in case it does not.
    # TODO(Nina): Add if-condition to check whether index is datetime or not.
    df["year"] = df.index.year
    df["month"] = df.index.month
    groupped = df.groupby(["full_symbol", "year", "month"])
    groupped_cnt = groupped.count()
    full_symbols = groupped_cnt.index.get_level_values(0).unique()
    for full_symbol in full_symbols:
        # TODO(Nina): Do not hardcode `target_col_name`
        target_col_name = "close"
        full_symbol_data = groupped_cnt[
            groupped_cnt.index.get_level_values(0) == full_symbol
        ]
        ax = full_symbol_data.plot.bar(
            y=target_col_name, rot=0, title=full_symbol
        )
        ax.hlines(
            y=threshold,
            xmin=-1,
            xmax=len(df),
            color="r",
        )
        # Get labels for x-axis.
        years = list(full_symbol_data.index.get_level_values(1).unique())
        months = list(full_symbol_data.index.get_level_values(2))
        ticklabels = [
            ".".join([str(year), str(month)])
            for year in years
            for month in months
        ]
        # Adjust tick lables.
        ax.xaxis.set_ticklabels(ticklabels)
        ax.figure.show()


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
full_symbol = ["binance::ADA_USDT", "binance::BTC_USDT"]
start_ts = pd.Timestamp("2018-08-17 00:00:00-0000")
end_ts = pd.Timestamp("2018-12-17 00:10:00-0000")
filter_data_mode = "assert"
data = ccxt_historical_client.read_data(
    full_symbol, start_ts, end_ts, columns=None, filter_data_mode=filter_data_mode
)
data.head(3)

# %%
start = pd.Timestamp("2018-08-01T00:00:00-00:00")
end = pd.Timestamp("2019-01-01T00:00:00-00:00")
plot_by_year_month(data, 15000, start_ts=start, end_ts=end)
