# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
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

# %% [markdown]
# # Description

# %% [markdown]
# This notebook is used to find currencies that are exactly the same in our universe.

# %% [markdown]
# # Imports

# %%
import logging
import os

import numpy as np
import pandas as pd
import pytz

import core.config.config_ as ccocon
import core.explore as cexp
import core.plotting as cplo
import helpers.datetime_ as hdatetim
import helpers.dbg as hdbg
import helpers.env as henv
import helpers.printing as hprintin
import helpers.s3 as hs3

import im.data.universe as imdatuni
import im.ccxt.data.load.loader as imccdaloloa

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprintin.config_notebook()


# %% [markdown]
# # Config

# %%
def get_config() -> ccocon.Config:
    """
    Get config that controls parameters.
    """
    config = ccocon.Config()
    # Load parameters.
    config.add_subconfig("load")
    config["load"]["aws_profile"] = "am"
    config["load"]["data_dir"] = os.path.join(hs3.get_path(), "data")
    # Data parameters.
    config.add_subconfig("data")
    config["data"]["close_price_col_name"] = "close"
    return config


config = get_config()
print(config)

# %% [markdown]
# # Find same currencies for CCXT

# %%
ccxt_loader = imccdaloloa.CcxtLoader(
    root_dir=config["load"]["data_dir"], aws_profile=config["load"]["aws_profile"]
)

# %%
ccxt_universe = imdatuni.get_trade_universe()["CCXT"]
ccxt_universe


# %% [markdown]
# ## Build a dataframe of returns for CCXT

# %%
def get_ccxt_returns(ccxt_universe, ccxt_loader, config):
    # Initialize lists of column names and returns series.
    colnames = []
    returns_srs_list = []
    # Iterate over exchange ids and currency pairs.
    for exchange_id in ccxt_universe:
        for curr_pair in ccxt_universe[exchange_id]:
            # Construct a colname from exchange id and currency pair.
            colname = " ".join([exchange_id, curr_pair])
            colnames.append(colname)
            # Extract historical data.
            data = ccxt_loader.read_data_from_filesystem(
                exchange_id=exchange_id, currency_pair=curr_pair, data_type="OHLCV"
            )
            # Get series of close prices.
            close_price_srs = data[config["data"]["close_price_col_name"]]
            # Remove values with duplicated indices.
            close_price_srs = close_price_srs[
                ~close_price_srs.index.duplicated(keep='last')
            ]
            # Get series of returns and append to the list.
            returns_srs = close_price_srs.diff()
            returns_srs_list.append(returns_srs)
    # Construct a dataframe and assign column names.
    df = pd.concat(returns_srs_list, axis=1)
    df.columns = colnames
    return df


# %%
df_returns = get_ccxt_returns(ccxt_universe, ccxt_loader, config)

# %%
df_returns.head()

# %%
corr_matrix = df_returns.corr()

# %% [markdown]
# On the heatmap we can clearly see that the same currency pairs at different exchanges are highly correlating.<br>
# It signifies that such approach can be used further on to find similar or sibling currencies.

# %%
cplo.plot_heatmap(corr_matrix)

# %% [markdown]
# Below we can see that the most correlated series are those that belong to the same currency pair.

# %% run_control={"marked": false}
# Display top 10 most correlated series for each currency pair.
for colname in corr_matrix.columns:
    corr_srs = corr_matrix[colname]
    corr_srs_sorted = corr_srs.sort_values(ascending=False)
    display(corr_srs_sorted.head(10))

# %% [markdown]
# # Remarks and possible improvements (for prod)

# %% [markdown]
# ## Dealing with bad spottiness

# %% [markdown]
# Despite the results, you could notice that Bitfinex data is correlating weakly compare to the same currencies from other exchanges, even though the duplicated observations have been removed so CmTask274 problem is not directly affecting this.
#
# The problem is in the spottiness of Bitfinex data which is just terrible - NaNs are all over the whole historical time period.<br>
# The problem with Bitfinex data quality is known and was first reflected in `im/ccxt/notebooks/CMTask13_Research_of_OHLCV_fetching_approaches_for_different_exchanges.ipynb`.<br>

# %% [markdown]
# Below you can find a code that displays it with even more clarity.<br>
# However, I'm having hard times to explain what I've done clearly - let's discuss it if I fail to deliver it.<br>
# The code is for prod and discussion, but can be converted to functions if needed.
#
# At first we use `.isna()` to convert all the values in the returns dataframe to bool values whether they are NaN or not NaN.<br>
# Then we convert them to integers so if a cell value was NaN its value becomes 1 and 0 vice verca.<br>
#
# Then we apply `.diff()` so each cell value means the following: 1 or -1 if a previous observation was NaN and the current is not NaN or vice verca and 0 if both current and previous observations were NaN or not NaN. In other words, each 1 or -1 will represent situations when a NaN value appears after a valid observations or vice versa. The more 1 or -1 values a column has, the more data gaps it contains so the worse its spottiness is.<br>
#
# To compute the amount of such gaps we drop the 1st row (which contains only NaNs after `.diff()`), use `.abs()` so all -1 values become 1 and sum. Resulting sum value will represent the number of data gaps for a currency pair and the higher it is the worse is the data quality.

# %%
df_returns.isna().astype(int).diff().dropna().abs().sum()

# %% [markdown]
# And now we see that Bitfinex is just the worst.
#
# Kucoin seems to have a lot of gaps too but we know from our previous research that Kucoin has gaps only before 2019-02-18 (see data gallery at https://docs.google.com/spreadsheets/d/1Tiiy1_qlKtq8Ay1qogIZwi55bYhnpGzRvt2RlrQUD00/edit#gid=0)<br>
#
# Let's see at data spottiness after this date.

# %%
# Checking a timezone to use.
df_returns.index[0]

# %%
df_returns[
    df_returns.index > pd.Timestamp("2019-02-20", tz='America/New_York')
].isna().astype(int).diff().dropna().abs().sum()

# %% [markdown]
# As you can see, Kucoin spottiness is just perfect after the specified date.<br>
# This is not the case for Bitfinex though (you can try different dates above - the result will still be disappointing).<br>
#
# This leads me to 2 suggestions:
# - What if we drop Bitfinex from our calculations at all until we research it in detail and clean its data
# - Replace all the Kucoin data with NaNs before the date when it's spottiness is perfect and probably do it for all the other exchanges
#
# What do you think?

# %% [markdown]
# Below I implement suggested measures (drop Bitfinex and crop Kucoin data) and look for equal currencies again.

# %%
# Drop Bitfinex data.
colnames2 = [col for col in df_returns if "bitfinex" not in col]
#
df_returns2 = df_returns[colnames2].copy()
df_returns2.head()

# %%
# Replace Kucoin data before 2019-02-20 with NaNs.
kucoin_colnames = [col for col in df_returns2 if "kucoin" in col]
#
df_returns2.loc[
    df_returns2.index < pd.Timestamp("2019-02-20", tz='America/New_York'),
    kucoin_colnames
] = np.nan

# %%
# Compute new spottiness indicator.
df_returns2.isna().astype(int).diff().dropna().abs().sum()

# %% [markdown]
# Much better now.

# %%
corr_matrix2 = df_returns2.corr()

# %%
cplo.plot_heatmap(corr_matrix2)

# %%
for colname in corr_matrix2.columns:
    corr_srs = corr_matrix2[colname]
    corr_srs_sorted = corr_srs.sort_values(ascending=False)
    display(corr_srs_sorted.head(10))

# %% [markdown]
# Now the same currency pairs are clustering more noticeably.

# %% [markdown]
# However, cleaning Kucoin data did not improve any results - it seems that the period of bad spottiness did not in fast impacted correlation computations much.<br>
# Therefore, for finding same currency pairs there is no big need to drop periods with bad spottiness if there is a significantly big period with perfect spottiness for a returns series.

# %%
for colname in kucoin_colnames:
    # Get stats series for raw and cleaned Kucoin data.
    old_stats = corr_matrix[colname].sort_values(ascending=False)
    new_stats = corr_matrix2[colname].sort_values(ascending=False)
    # Combine in df and display.
    stats_df = pd.concat([old_stats, new_stats], axis = 1)
    stats_df.columns = ["old_stats", "new_stats"]
    display(stats_df.head(10))

# %% [markdown]
# ## Picking cutoff correlation values

# %% [markdown]
# For the currency pairs chosen for this research it is quite easy to find same ones just by name.<br>
# Clearly, they have the highest levels of correlation among each other, but I guess we don't want to rely only on names but on certain correlation levels.<br>
#
# However, some equal currency pairs at different exchanges have lower levels of correlation between each other than some different stable coins.
#
# E.g. the correlation between `binance AVAX/USDT` and `kucoin AVAX/USDT` is 0.72 while the correlation between `binance BTC/USDT` and `binance ETH/USDT` is 0.76.<br>
#
# How do we set a cutoff value for a correlation between a pair of currency pair returns to consider them equal?<br>
# Do we really need it at all?<br>
#
# I'll also check my computations for mistakes again but seems that it won't drop the problem. 

# %%
display(corr_matrix2["binance AVAX/USDT"].sort_values(ascending=False))

# %%
display(corr_matrix2["binance BTC/USDT"].sort_values(ascending=False))

# %%
