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

import core.config.config_ as cconconf
import helpers.dbg as hdbg
import helpers.env as henv
import helpers.printing as hprint
import helpers.s3 as hs3
import im.cryptodatadownload.data.load.loader as imcdalolo
import im_v2.ccxt.data.client.clients as imvcdclcl
import im_v2.ccxt.universe.universe as imvccunun
import im_v2.common.data.client.clients as ivcdclcl
import research_amp.cc.statistics as ramccsta

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()


# %% [markdown]
# # Configs

# %%
# Two configs are necessary in this situation because current downloading functions
# work only with specific 'vendor' value.

# %%
def get_cmtask324_config_ccxt() -> cconconf.Config:
    """
    Get task232-specific config.
    """
    config = cconconf.Config()
    # Load parameters.
    config.add_subconfig("load")
    config["load"]["aws_profile"] = "am"
    config["load"]["data_dir"] = os.path.join(hs3.get_path(), "data")
    # Data parameters.
    config.add_subconfig("data")
    config["data"]["data_type"] = "OHLCV"
    config["data"]["target_frequency"] = "T"
    config["data"]["universe_version"] = "v03"
    config["data"]["vendor"] = "CCXT"
    # Column names.
    config.add_subconfig("column_names")
    config["column_names"]["close_price"] = "close"
    config["column_names"]["currency_pair"] = "currency_pair"
    config["column_names"]["exchange_id"] = "exchange_id"
    return config


# %%
config_ccxt = get_cmtask324_config_ccxt()
print(config_ccxt)


# %%
def get_cmtask324_config_cdd() -> cconconf.Config:
    """
    Get task324-specific config.
    """
    config = cconconf.Config()
    # Load parameters.
    config.add_subconfig("load")
    config["load"]["aws_profile"] = "am"
    config["load"]["data_dir"] = os.path.join(hs3.get_path(), "data")
    # Data parameters.
    config.add_subconfig("data")
    config["data"]["data_type"] = "OHLCV"
    config["data"]["target_frequency"] = "T"
    config["data"]["universe_version"] = "v01"
    config["data"]["vendor"] = "CDD"
    # Column names.
    config.add_subconfig("column_names")
    config["column_names"]["close_price"] = "close"
    config["column_names"]["currency_pair"] = "currency_pair"
    config["column_names"]["exchange_id"] = "exchange_id"
    return config


# %%
config_cdd = get_cmtask324_config_cdd()
print(config_cdd)

# %% [markdown]
# # Load the data universe

# %% [markdown]
# ## CCXT

# %%
ccxt_universe = imvccunun.get_vendor_universe(version="v03")

# %% [markdown]
# ## CDD

# %% run_control={"marked": false}
cdd_universe = imvccunun.get_vendor_universe(version="v01", vendor="CDD")
# Remove non-USDT elements, since we are not interested in them.
cdd_universe = [element for element in cdd_universe if element.endswith("USDT")]

# %% [markdown]
# # Compare universes

# %%
_LOG.info("Number of full symbols in CCXT: %s", len(ccxt_universe))
_LOG.info("Number of full symbols in CDD: %s", len(cdd_universe))

# %%
# Intersection of full symbols between two vendors.
currency_pair_intersection = set(ccxt_universe).intersection(cdd_universe)
_LOG.info("Number of similar full symbols: %s", len(currency_pair_intersection))
display(currency_pair_intersection)

# %%
# Full symbols that are included in CCXT but not in CDD.
ccxt_and_not_cdd = set(ccxt_universe).difference(cdd_universe)
_LOG.info(
    "Number of full symbols that are included in CCXT but not in CDD: %s",
    len(ccxt_and_not_cdd),
)
display(ccxt_and_not_cdd)

# %%
# Full symbols that are included in CDD but not in CCXT.
cdd_and_not_ccxt = set(cdd_universe).difference(ccxt_universe)
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
    element for element in cdd_universe if element.startswith("binance")
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
cdd_loader = imcdalolo.CddLoader(
    data_type="ohlcv", root_dir=root_dir, aws_profile="am"
)

for full_symbol in currency_pair_intersection_binance:
    cur_data = cdd_loader.read_data(full_symbol=full_symbol)
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
    Resample close price on the currency level to the specified frequency using
    the last close price.

    :param df: OHLCV data
    :param resampling_freq: frequency from `pd.date_range()` to resample to
    :return: resampled close price per currency
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
    Take CCXT and CDD close prices and calculate the correlations for each
    specific currency pair.

    :param ccxt_series: resampled close price per currency for CCXT
    :param cdd_series: resampled close price per currency for CDD
    :param compute_returns: if True - compare returns, if False - compare close prices
    :return: correlation matrix per currency
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

# %% [markdown]
# ### 1-day close prices

# %%
close_corr_1day = calculate_correlations(
    ccxt_binance_series_1d, cdd_binance_series_1d, compute_returns=False
)
display(close_corr_1day)

# %% [markdown]
# ### 5-min close prices

# %%
close_corr_5min = calculate_correlations(
    ccxt_binance_series_5min, cdd_binance_series_5min, compute_returns=False
)
display(close_corr_5min)

# %% [markdown]
# # Statistical properties of a full symbol in CDD

# %%
# Clearing CDD currency pairs that are incorrect.

# Binance
cdd_universe.remove("binance::SCU_USDT")

# ftx has some critical mistakes in the downloading process, so can not continue analysis with them.
# see CMTask801 - Downloading issues of FTX exchange from CDD universe for further reference.
cdd_ftx_universe = [
    element for element in cdd_universe if element.startswith("ftx")
]
for elem in cdd_ftx_universe:
    cdd_universe.remove(elem)

# kucoin exchange: the timestamps are obviously wrong and with too short time period.
# see CMTask253 - Fix timestamp for CDD - kucoin for reference.
cdd_kucoin_universe = [
    element for element in cdd_universe if element.startswith("kucoin")
]
for elem in cdd_kucoin_universe:
    cdd_universe.remove(elem)

# %% [markdown]
# ## Comparison of intersection of full symbols between CCXT and CDD

# %%
# Full symbols that are included in CDD but not in CCXT (cleaned from unavailable full symbols).
cdd_and_ccxt_cleaned = set(ccxt_universe).intersection(cdd_universe)
len(cdd_and_ccxt_cleaned)

# %% [markdown]
# ### Load the intersection of full symbols for CDD and CCXT

# %% [markdown]
# #### CDD

# %%
compute_start_end_stats = lambda data: ramccsta.compute_start_end_stats(
    data, config_cdd
)

cdd_start_end_table = ramccsta.compute_stats_for_universe(
    cdd_and_ccxt_cleaned, config_cdd, compute_start_end_stats
)

# %%
# CDD names cleaning.
cdd_start_end_table["currency_pair"] = cdd_start_end_table[
    "currency_pair"
].str.replace("/", "_")

# %%
cdd_start_end_table.head(3)

# %% [markdown]
# #### CCXT

# %%
compute_start_end_stats = lambda data: ramccsta.compute_start_end_stats(
    data, config_ccxt
)

ccxt_start_end_table = ramccsta.compute_stats_for_universe(
    cdd_and_ccxt_cleaned, config_ccxt, compute_start_end_stats
)

# %%
ccxt_start_end_table.head(3)


# %% [markdown]
# ### Display the union results

# %%
def unify_start_end_tables(
    cdd_df: pd.DataFrame, ccxt_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Combine CCXT and CDD start-end stats tables into unique table.

    :param cdd_df: start-end table for CCXT
    :param ccxt_df: start-end table for CDD
    :return: unified start-end table
    """
    # set Multiindex.
    cdd_df = cdd_df.set_index(["exchange_id", "currency_pair"])
    ccxt_df = ccxt_df.set_index(["exchange_id", "currency_pair"])
    # add suffixes.
    ccxt_df = ccxt_df.add_suffix("_ccxt")
    cdd_df = cdd_df.add_suffix("_cdd")
    # combine two universes.
    ccxt_and_cdd = pd.concat([cdd_df, ccxt_df], axis=1)
    # sort columns.
    cols_to_sort = ccxt_and_cdd.columns.to_list()
    ccxt_and_cdd = ccxt_and_cdd[sorted(cols_to_sort)]
    return ccxt_and_cdd


# %%
union_cdd_ccxt_stats = unify_start_end_tables(
    cdd_start_end_table, ccxt_start_end_table
)
display(union_cdd_ccxt_stats)

# %% [markdown]
# ## Comparison of full symbols that are included in CDD but not available in CCXT

# %%
# Set of full symbols that are included in CDD but not available in CCXT (cleaned from unavailable full symbols).
cdd_and_not_ccxt_cleaned = set(cdd_universe).difference(ccxt_universe)
len(cdd_and_not_ccxt_cleaned)

# %%
# for 'avg_data_points_per_day' the amount of "days_available" is equal to 0, so it crashes the calculations.
cdd_and_not_ccxt_cleaned.remove("binance::DAI_USDT")

# %%
compute_start_end_stats = lambda data: ramccsta.compute_start_end_stats(
    data, config_cdd
)

cdd_unique_start_end_table = ramccsta.compute_stats_for_universe(
    cdd_and_not_ccxt_cleaned, config_cdd, compute_start_end_stats
)

# %%
display(cdd_unique_start_end_table)
