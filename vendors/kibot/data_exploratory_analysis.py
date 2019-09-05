# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.2'
#       jupytext_version: 1.2.1
#   kernelspec:
#     display_name: Python [conda env:develop] *
#     language: python
#     name: conda-env-develop-py
# ---

# %% [markdown]
# ## Import

# %%
# %load_ext autoreload
# %autoreload 2
import datetime
import logging
import os
import platform

import numpy as np
import pandas as pd
import seaborn as sns
import scipy
import matplotlib
import matplotlib.pyplot as plt
import sklearn

import helpers.config as cfg
import helpers.dbg as dbg
import core.finance as fin
import helpers.printing as printing
import core.explore as exp

import vendors.kibot.utils as kut

# %%
print(cfg.get_system_signature())

printing.config_notebook()

#dbg.init_logger(verb=logging.DEBUG)
dbg.init_logger(verb=logging.INFO)
#dbg.test_logger()

_LOG = logging.getLogger(__name__)

# %% [markdown]
# # Metadata

# %% [markdown]
# ## Read misc metadata

# %%
df1 = kut.read_metadata1()
df1.head(3)

# %%
df2 = kut.read_metadata2()
df2.head(3)

# %%
df3 = kut.read_metadata3()
df3.head(3)

# %%
df4 = kut.read_metadata4()
print(df4.head(3))

print(df4["Exchange"].unique())

# %% [markdown]
# ## Explore metadata

# %%
mask = ["GAS" in d or "OIL" in d for d in df4["Description"]]
print(sum(mask))
print(df4[mask].drop(["SymbolBase", "Size(MB)"], axis=1))

# %% [markdown]
# # Price data

# %% [markdown]
# ## Read continuous daily prices for single futures

# %%
s = "CL"
#nrows = None
nrows = 10000
#file_name = "s3://alphamatic/kibot/All_Futures_Contracts_1min/%s.csv.gz" % s
file_name = "s3://alphamatic/kibot/All_Futures_Continuous_Contracts_daily/%s.csv.gz" % s
df = kut.read_data_memcached(file_name, nrows)
df.head(3)

# %% [markdown]
# ## Read continuous 1-min prices for single futures

# %%
s = "CL"
#nrows = None
nrows = 10000
#file_name = "s3://alphamatic/kibot/All_Futures_Contracts_1min/%s.csv.gz" % s
file_name = "s3://alphamatic/kibot/All_Futures_Continuous_Contracts_1min/%s.csv.gz" % s
df = kut.read_data_memcached(file_name, nrows)
df.head(3)

# %%
## Read continuous 1-min prices for multiple futures

# %% [markdown]
# ## Read continuous daily prices for multiple futures

# %%
symbols = "CL NG RB BZ".split()
file_name = "s3://alphamatic/kibot/All_Futures_Continuous_Contracts_daily/%s.csv.gz"
nrows = 10000

daily_price_dict_df = kut.read_multiple_symbol_data(symbols, file_name, nrows=nrows)

daily_price_dict_df["CL"].head(3)

# %% [markdown]
# ## Read continuous 1-min prices for multiple futures

# %%
symbols = "CL NG RB BZ".split()
file_name = "s3://alphamatic/kibot/All_Futures_Continuous_Contracts_1min/%s.csv.gz"
nrows = 10000

daily_price_dict_df = kut.read_multiple_symbol_data(symbols, file_name, nrows=nrows)

daily_price_dict_df["CL"].head(3)

# %% [markdown]
# ## Read data through config API

# %%
import collections

config = collections.OrderedDict()

if "__CONFIG__" in os.environ:
    config = os.environ["__CONFIG__"]
    _LOG.info("__CONFIG__=%s", config)
    config = eval(config)
else:
    # Use the data from S3.
    file_name = "s3://alphamatic/kibot/All_Futures_Contracts_1min/ES.csv.gz"
    config["file_name"] = file_name
    config["nrows"] = 100000

_LOG.info(cfg.config_to_string(config))

# %%
df = kut.read_data_from_config(config)

_LOG.info("df.shape=%s", df.shape)
_LOG.info("datetimes=[%s, %s]", df.index[0], df.index[-1])
_LOG.info("df=\n%s", df.head(3))

# %% [markdown]
# ## Read raw data directly from S3

# %%
s = "CL"
file_name = "s3://alphamatic/kibot/All_Futures_Continuous_Contracts_1min/%s.csv.gz" % s
nrows = 10000

df = pd.read_csv(file_name, header=None, parse_dates=[0], nrows=nrows)
# df.columns = "datetime open high low close vol".split()
df.head(3)

# %% [markdown]
# # Return computation

# %% [markdown]
# ## 1-min for single futures

# %%
# TODO(gp)

# %% [markdown]
# ## 1-min for multiple futures

# %%
# Read multiple futures.
symbols = "CL NG RB BZ".split()
file_name = "s3://alphamatic/kibot/All_Futures_Continuous_Contracts_1min/%s.csv.gz"
nrows = 100000
min_price_dict_df = kut.read_multiple_symbol_data(symbols, file_name, nrows=nrows)

_LOG.info("keys=%s", min_price_dict_df.keys())
min_price_dict_df["CL"].tail(3)

# %% [markdown]
# ### Compute returns ret_0

# %%
mode = "pct_change"
min_rets = kut.compute_ret_0_from_multiple_1min_prices(min_price_dict_df, mode)

min_rets.head(3)

# %%
min_rets.fillna(0.0).resample("1D").sum().cumsum().plot()

# %% [markdown]
# ### Resample to 1min

# %%
# Resample to 1min.
_LOG.info("## Before resampling")
exp.report_zero_null_stats(min_rets)

# %%
exp.plot_non_na_cols(min_rets.resample("1D").sum())

# %%
min_rets = fin.resample_1min(min_rets)

_LOG.info("## After resampling")
exp.report_zero_null_stats(min_rets)

min_rets.fillna(0.0, inplace=True)

# %% [markdown]
# ### z-scoring

# %%
zscore_com = 28
min_zrets = fin.zscore(
    min_rets, com=zscore_com, demean=False, standardize=True, delay=1)
min_zrets.columns = [c.replace("ret_", "zret_") for c in min_zrets.columns]
min_zrets.dropna().head(3)

# %%
min_zrets.fillna(0.0).resample("1D").sum().cumsum().plot()

# %%
annot = True
stocks_corr = min_rets.dropna().corr()

sns.clustermap(stocks_corr, annot=annot)

# %% [markdown]
# ## Daily for single futures

# %% [markdown]
# ## Daily for multiple futures
