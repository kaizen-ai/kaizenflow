# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.2'
#       jupytext_version: 1.2.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
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
import helpers.printing as printing
import core.explore as exp
import core.finance as fin

import vendors.kibot.utils as kut

# %%
print(cfg.get_system_signature())

printing.config_notebook()

# TODO(gp): Changing level during the notebook execution doesn't work. Fix it.
#dbg.init_logger(verb=logging.DEBUG)
dbg.init_logger(verb=logging.INFO)
#dbg.test_logger()

_LOG = logging.getLogger(__name__)

# %% [markdown]
# # Metadata

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

# %%
df4[mask]['Symbol'].values

# %% [markdown]
# # Read data

# %%
import collections

config = collections.OrderedDict()

if "__CONFIG__" in os.environ:
    config = os.environ["__CONFIG__"]
    print("__CONFIG__=", config)
    config = eval(config)
else:
    #config["nrows"] = 100000
    config["nrows"] = None
    #
    config["zscore_com"] = 28

print(cfg.config_to_string(config))

# %% [markdown]
# # Prices

# %% [markdown]
# ## Read daily prices

# %%
all_symbols = [
    futures.replace('.csv.gz', '') for futures in os.listdir(
        '/data/kibot/All_Futures_Continuous_Contracts_daily')
]

# %%
symbols = df4[mask]['Symbol'].values
symbols

# %%
file_name = "/data/kibot/All_Futures_Continuous_Contracts_daily/%s.csv.gz"

daily_price_dict_df = kut.read_multiple_symbol_data(symbols, file_name, nrows=config["nrows"])

daily_price_dict_df["CL"].tail(2)

# %% [markdown]
# # Top futures by volume

# %% [markdown]
# ## Sum volume

# %%
daily_volume_sum_dict = {
    symbol: daily_prices_symbol['vol'].sum()
    for symbol, daily_prices_symbol in daily_price_dict_df.items()
}

# %%
daily_volume_sum_df = pd.DataFrame.from_dict(daily_volume_sum_dict, orient='index', columns=['sum_vol'])
daily_volume_sum_df.index.name = 'symbol'

# %%
daily_volume_sum_df.sort_values('sum_vol', ascending=False)

# %% [markdown]
# ## Mean volume

# %%
daily_volume_mean_dict = {
    symbol: daily_prices_symbol['vol'].mean()
    for symbol, daily_prices_symbol in daily_price_dict_df.items()
}

# %%
daily_volume_mean_df = pd.DataFrame.from_dict(daily_volume_mean_dict, orient='index', columns=['mean_vol'])
daily_volume_mean_df.index.name = 'symbol'

# %%
daily_volume_mean_df.sort_values('mean_vol', ascending=False)

# %%
