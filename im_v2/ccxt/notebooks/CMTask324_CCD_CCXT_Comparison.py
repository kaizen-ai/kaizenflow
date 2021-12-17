# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.13.3
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Imports

# %%
import logging
import os

import pandas as pd

import helpers.dbg as hdbg
import helpers.env as henv
import helpers.printing as hprint
import helpers.s3 as hs3
import im.cryptodatadownload.data.load.loader as imcdalolo
import im_v2.ccxt.data.client.clients as imvcdclcl
import im_v2.ccxt.universe.universe as imvccunun
import im_v2.common.data.client.clients as ivcdclcl

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %% [markdown]
# # Load the data universe

# %% [markdown]
# ## CCXT

# %%
ccxt_universe = imvccunun.get_vendor_universe(version="v03")

# %% [markdown]
# ## CDD

# %% run_control={"marked": false}
universe_cdd = imvccunun.get_vendor_universe(version="v01", vendor="CDD")

# %% [markdown]
# # Compare universes

# %%
_LOG.info("Number of full symbols in CCXT: %s", len(ccxt_universe))
_LOG.info("Number of full symbols in CDD: %s", len(universe_cdd))

# %%
# Intersection of full symbols between two vendors.
currency_pair_intersection = set(ccxt_universe).intersection(universe_cdd)
_LOG.info("Number of similar full symbols: %s", len(currency_pair_intersection))
display(currency_pair_intersection)

# %%
# Full symbols that are included in CCXT but not in CDD.
ccxt_and_not_cdd = set(ccxt_universe).difference(universe_cdd)
_LOG.info(
    "Number of full symbols that are included in CCXT but not in CDD: %s",
    len(ccxt_and_not_cdd),
)
display(ccxt_and_not_cdd)

# %%
# Full symbols that are included in CDD but not in CCXT.
cdd_and_not_ccxt = set(universe_cdd).difference(ccxt_universe)
_LOG.info(
    "Number of full symbols that are included in CDD but not in CCXT: %s",
    len(cdd_and_not_ccxt),
)
display(cdd_and_not_ccxt)

# %% [markdown]
# # Compare close prices / returns from Binance

# %% [markdown]
# ## Load the data

# %% [markdown]
# The code below can be used to load all the existing data from two vendors CDD and CCXT. Current version is specified to Binance only, however, even for one exchange there's too many data to operate, that's why the output is the intersection of currency pairs between to universe, since one can compare only the intersection of currency pairs for two vendors.

# %%
# Load Binance-specific universe for CCXT.
ccxt_binance_universe = [
    element for element in ccxt_universe if element.startswith("binance")
]
# Load Binnance-specific universe for CDD.
cdd_binance_universe_initial = [
    element for element in universe_cdd if element.startswith("binance")
]
cdd_binance_universe = cdd_binance_universe_initial.copy()
# SCU_USDT has incorrect columns, so can not be downloaded.
# See CMTask244 - Cannot load CDD - binance - SCU/USDT from s3 for the reference.
cdd_binance_universe.remove("binance::SCU_USDT")
# The intersection of Binance currency pairs from two universes.
currency_pair_intersection_binance = set(ccxt_binance_universe).intersection(
    cdd_binance_universe_initial
)

# %%
root_dir = os.path.join(hs3.get_path(), "data")

# %%
cdd_data = []
cdd_loader = imcdalolo.CddLoader(root_dir=root_dir, aws_profile="am")

for full_symbol in currency_pair_intersection_binance:
    _, currency_pair = ivcdclcl.parse_full_symbol(full_symbol)
    cur_data = cdd_loader.read_data_from_filesystem(
        exchange_id="binance", currency_pair=currency_pair, data_type="ohlcv"
    )
    cdd_data.append(cur_data)
cdd_binance_df = pd.concat(cdd_data)

# %%
display(cdd_binance_df.head(3))
display(cdd_binance_df.shape)

# %%
ccxt_client = imvcdclcl.CcxtCsvFileSystemClient(
    data_type="ohlcv", root_dir=root_dir, aws_profile="am"
)
multiple_symbols_client = ivcdclcl.MultipleSymbolsImClient(
    class_=ccxt_client, mode="concat"
)
ccxt_binance_df = multiple_symbols_client.read_data(
    currency_pair_intersection_binance
)

# %%
ccxt_binance_df = ccxt_binance_df.sort_index()

# %%
display(ccxt_binance_df.head(3))
display(ccxt_binance_df.shape)

# %% [markdown]
# ## Calculate returns and correlation

# %%
# CDD names cleaning.
cdd_binance_df["currency_pair"] = cdd_binance_df["currency_pair"].str.replace(
    "/", "_"
)


# %%
def resample_close_price(df: pd.DataFrame, resampling_freq: str) -> pd.Series:
    """
    Transform OHLCV data to the grouped series with resampled frequency and
    last close prices.

    :param df: OHLCV data
    :param resampling_freq: frequency from `pd.date_range()` to resample to
    :return: grouped and resampled close prices
    """
    # Reseting DateTime index, since pd.Grouper can't use index values.
    df = df.reset_index().rename(columns={"index": "stamp"})
    # Group by currency pairs and simultaneously resample to the desired frequency.
    resampler = df.groupby(
        ["currency_pair", pd.Grouper(key="stamp", freq=resampling_freq)]
    )
    # Take the last close value from each resampling period.
    close_series = resampler.close.last()
    return close_series


# %%
def calculate_correlations(
    ccxt_close_price: pd.Series, cdd_close_price: pd.Series, compute_returns: bool
) -> pd.DataFrame:
    """
    Take two series with close prices(i.e. CDD and CCXT data) and calculate the
    correlations for each specific currency pair.

    :param ccxt_series: grouped and resampled close prices for CCXT
    :param cdd_series: grouped and resampled close prices for CDD
    :param compute_returns: if True - compare returns, if False - compare close prices
    :return: grouped correlation matrix
    """
    if compute_returns:
        # Group by currency pairs in order to calculate the percentage returns.
        grouper_cdd = cdd_close_price.groupby("currency_pair")
        cdd_close_price = grouper_cdd.pct_change()
        grouper_ccxt = ccxt_close_price.groupby("currency_pair")
        ccxt_close_price = grouper_ccxt.pct_change()
    # Combine and calculate correlations.
    combined = pd.merge(
        cdd_close_price, ccxt_close_price, left_index=True, right_index=True
    )
    # Rename the columns.
    if compute_returns:
        combined.columns = ["ccxt_returns", "cdd_returns"]
    else:
        combined.columns = ["cdd_close", "ccxt_close"]
    # Group by again to calculte returns correlation for each currency pair.
    corr_matrix = combined.groupby(level=0).corr()
    return corr_matrix


# %%
# Corresponding resampled Series.
ccxt_binance_series_1d = resample_close_price(ccxt_binance_df, "1D")
cdd_binance_series_1d = resample_close_price(cdd_binance_df, "1D")

ccxt_binance_series_5min = resample_close_price(ccxt_binance_df, "5min")
cdd_binance_series_5min = resample_close_price(cdd_binance_df, "5min")

# %% [markdown]
# ### 1-day returns

# %%
returns_corr_1day = calculate_correlations(
    ccxt_binance_series_1d, cdd_binance_series_1d, compute_returns=True
)
display(returns_corr_1day)

# %% [markdown]
# ### 5-min returns

# %%
returns_corr_5min = calculate_correlations(
    ccxt_binance_series_5min, cdd_binance_series_5min, compute_returns=True
)
display(returns_corr_5min)

# %% [markdown]
# ## Compare close prices

# %%
close_corr_1day = calculate_correlations(
    ccxt_binance_series_1d, cdd_binance_series_1d, compute_returns=False
)
display(close_corr_1day)
