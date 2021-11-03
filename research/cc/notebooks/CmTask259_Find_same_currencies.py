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
from typing import Dict, List, Union

import numpy as np
import pandas as pd
import seaborn as sns

import core.config.config_ as ccocon
import core.plotting as cplo
import core.statistics as csta
import helpers.dbg as hdbg
import helpers.env as henv
import helpers.hpandas as hhpandas
import helpers.printing as hprintin
import helpers.s3 as hs3
import im.ccxt.data.load.loader as imccdaloloa
import im.data.universe as imdauni

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
    config["data"]["freq"] = "T"
    config["data"]["universe_version"] = "v0_1"
    return config


config = get_config()
print(config)


# %% [markdown]
# # Functions

# %%
def find_longest_not_nan_sequence(data: Union[pd.Series, pd.DataFrame]):
    """
    Find the longest sequence of not-NaN values in a series or dataframe.

    For a dataframe the longest sequence of rows with no NaN values is returned.

    :param data: input series or dataframe
    :return: longest sequence of not-NaN values
    """
    # Verify that index is monotonically increasing.
    hhpandas.dassert_strictly_increasing_index(data)
    # Get index frequency.
    freq = pd.infer_freq(data.index)
    # Get indices of only not-NaN values.
    not_nan_index = np.array(data.dropna().index)
    # Get a mask to distinguish not-NaN values that are further from their
    # not-NaN precedent than 1 frequency time step.
    mask = np.where(np.diff(not_nan_index) != pd.Timedelta(1, freq))[0] + 1
    # Get the longest monotonically increasing sequence of indices.
    longest_not_nan_index = max(np.split(not_nan_index, mask), key=len)
    # Get the longest sequence of not-NaN values.
    longest_not_nan_seq = data.loc[longest_not_nan_index].copy()
    return longest_not_nan_seq


def compute_longest_not_nan_sequence_stats(df: pd.DataFrame):
    """
    Compute stats about the longest not-NaN sequence in each dataframe column.

    :param df: input dataframe
    :return: stats dataframe
    """
    # Initiate results dict.
    res_dict = {}
    # Iterate over each series in columns.
    for colname in df.columns:
        col_srs = df[colname].copy()
        # Remove leading and trailing NaNs.
        first_idx = col_srs.first_valid_index()
        last_idx = col_srs.last_valid_index()
        col_srs = col_srs[first_idx:last_idx].copy()
        # Get the longest not-NaN sequence in a series.
        longest_not_nan_seq = find_longest_not_nan_sequence(col_srs)
        # Compute necessary stats and put in a list.
        stats = [
            100 * (1 - csta.compute_frac_nan(col_srs)),
            (last_idx - first_idx).days,
            100 * (len(longest_not_nan_seq) / len(col_srs)),
            longest_not_nan_seq.index[0],
            longest_not_nan_seq.index[-1],
        ]
        # Append stats list to the result dict under a column name key.
        res_dict[colname] = stats
    # Build a dataframe from the result dict.
    res_df = pd.DataFrame.from_dict(
        res_dict,
        orient="index",
        columns=[
            "coverage",
            "longest_seq_days_available",
            "share_of_longest_seq",
            "longest_seq_start_date",
            "longest_seq_end_date",
        ],
    )
    # Sort by coverage and share of longest not-NaN sequence.
    res_df = res_df.sort_values(by=["coverage", "share_of_longest_seq"])
    return res_df


def get_ccxt_price_df(
    ccxt_universe: Dict[str, List[str]],
    ccxt_loader: imccdaloloa.CcxtLoader,
    config: ccocon.Config,
):
    """
    Read price data from CCXT for a given universe using the given loader.

    :param ccxt_universe: CCXT trade universe
    :param ccxt_loader: CCXT loader
    :param config: parameters config
    :return: price data for a given universe
    """
    # Initialize lists of column names and returns series.
    colnames = []
    price_srs_list = []
    # Iterate over exchange ids and currency pairs.
    for exchange_id in ccxt_universe:
        for curr_pair in ccxt_universe[exchange_id]:
            # Construct a colname from exchange id and currency pair.
            colname = " ".join([exchange_id, curr_pair])
            colnames.append(colname)
            # Extract historical data.
            data = ccxt_loader.read_data_from_filesystem(
                exchange_id=exchange_id,
                currency_pair=curr_pair,
                data_type="OHLCV",
            )
            # TODO(Dan): Deprecate after CmTask298 is resolved.
            # Drop duplicates.
            data = data.drop_duplicates()
            # Get series of prices and append to the list.
            price_srs = data[config["data"]["close_price_col_name"]]
            price_srs_list.append(price_srs)
    # Construct a dataframe and assign column names.
    df = pd.concat(price_srs_list, axis=1)
    df.columns = colnames
    # Resample to the specified frequency.
    df = hhpandas.resample_df(df, config["data"]["freq"])
    return df


# %% [markdown]
# # Find the longest not NaN sequence in return series and narrow down trade universe

# %%
ccxt_loader = imccdaloloa.CcxtLoader(
    root_dir=config["load"]["data_dir"], aws_profile=config["load"]["aws_profile"]
)

# %%
ccxt_universe = imdauni.get_trade_universe(
    config["data"]["universe_version"]
)["CCXT"]
ccxt_universe

# %%
df_price = get_ccxt_price_df(ccxt_universe, ccxt_loader, config)
df_price.head(3)

# %%
df_price.describe().round(2)

# %%
df_price.head()

# %% [markdown]
# Below is a code chunk that I did not factor out since it rather should be a part of some stats function like `compute_start_end_table()` from `CMTask232_compute_start_end_table.ipynb`.<br>
#
# This code is applying `find_longest_not_nan_sequence()` to all the gathered price series and outputs the longest not-NaN sequence time interval, share of its length to the length of original series and coverage.<br>
#
# Looking at the results we can see that all the exchanges except for Bitfinex have significantly big longest not-NaN sequence (>13% at least) in combine with high data coverage (>85%). Bitfinex has a very low data coverage and its longest not-NaN sequences are less than 1% compare to the original data length which means that Bitfinex data spottiness is too scattered and we should exclude it from our analysis until we get clearer data for it.

# %%
df_stats = compute_longest_not_nan_sequence_stats(df_price)
df_stats

# %%
# Remove observations related to Bitfinex.
colnames = [col for col in df_price if "bitfinex" not in col]
df_price = df_price[colnames].copy()

# %% [markdown]
# # Find same currencies for CCXT

# %%
df_returns = df_price.pct_change()
df_returns.head(3)

# %%
corr_matrix = df_returns.corr()
_ = cplo.plot_heatmap(corr_matrix)

# %% [markdown]
# `cluster_and_select()` distinguishes clusters but some very highly correlated stable coins are clustered together so it seems like that we cannot rely on dendrodram and clustering alone.

# %%
_ = cplo.cluster_and_select(df_returns, 11)

# %% run_control={"marked": false}
_ = sns.clustermap(corr_matrix, figsize=(20, 20))

# %% run_control={"marked": false}
# Display top 10 most correlated series for each currency pair.
for colname in corr_matrix.columns:
    corr_srs = corr_matrix[colname]
    corr_srs_sorted = corr_srs.sort_values(ascending=False)
    display(corr_srs_sorted.head(10))

# %% [markdown]
# # Calculations on data resampled to 1 day

# %%
df_price_1day = df_price.resample("D", closed="right", label="right").mean()
df_price_1day.head(3)

# %%
df_returns_1day = df_price_1day.pct_change()
df_returns_1day.head(3)

# %%
corr_matrix_1day = df_returns_1day.corr()
_ = cplo.plot_heatmap(corr_matrix_1day)

# %% [markdown]
# Resampling to 1 day makes clusters much more visible. <br>
# If we take a look at correlation numbers, we can see that equal currencies on different exchanges have a correlation above ~0.94 while different currencies correlate at much less rate.
#
# Therefore, it seems that for detecting similar currencies we'd better use 1 day frequency.

# %%
_ = cplo.cluster_and_select(df_returns_1day, 11)

# %%
_ = sns.clustermap(corr_matrix_1day, figsize=(20, 20))

# %%
# Display top 10 most correlated series for each currency pair.
for colname in corr_matrix_1day.columns:
    corr_srs = corr_matrix_1day[colname]
    corr_srs_sorted = corr_srs.sort_values(ascending=False)
    display(corr_srs_sorted.head(10))

# %%
