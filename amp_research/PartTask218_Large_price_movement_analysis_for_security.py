# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.2'
#       jupytext_version: 1.2.4
#   kernelspec:
#     display_name: Python [conda env:.conda-p1_develop] *
#     language: python
#     name: conda-env-.conda-p1_develop-py
# ---

# %% [markdown]
# ## Imports

# %%
# %load_ext autoreload
# %autoreload 2

import logging
import os

import pandas as pd
from matplotlib import pyplot as plt

import amp_research.price_movement_analysis as pma
import helpers.dbg as dbg
import helpers.env as env
import helpers.printing as pri
import helpers.s3 as hs3
import vendors.kibot.utils as kut

# %%
print(env.get_system_signature())

pri.config_notebook()

dbg.init_logger(verb=logging.INFO)

_LOG = logging.getLogger(__name__)

# %% [markdown]
# ## Helper functions

# %%
SYMBOL = "CL"


# %%
def get_top_100(series):
    return series.sort_values(ascending=False).head(100)


# %% [markdown]
# # Load daily and minutely data

# %%
# Daily data.
s3_path = hs3.get_path()
kibot_path = os.path.join(
    s3_path, "kibot/All_Futures_Continuous_Contracts_daily/%s.csv.gz"
)
file_name = kibot_path % SYMBOL

daily_prices = kut.read_data(file_name, nrows=None)

daily_prices.tail(2)

# %%
# Minute data.
s3_path = hs3.get_path()
kibot_path = os.path.join(
    s3_path, "kibot/All_Futures_Continuous_Contracts_1min/%s.csv.gz"
)
file_name = kibot_path % SYMBOL
minutely_prices = kut.read_data(file_name, nrows=None)

minutely_prices.tail(2)

# %%
# TODO(Julia): Should we move the code to downsample in kut?
five_min_prices = minutely_prices.resample("5Min").last()

# %%
five_min_prices.head()

# %% [markdown]
# # Daily price movements

# %%
tau = 18
zscored_rets = pma.get_zscored_returns(daily_prices, "daily", tau=tau)

zscored_rets = zscored_rets.abs()

top_daily_movements = get_top_100(zscored_rets)

top_daily_movements.head(10)

# %%
top_daily_movements.index.year.value_counts(sort=False).plot(kind="bar")
plt.title("How many of the top-100 price movements occured during each year")
plt.show()

# %%
top_daily_movements_by_year = zscored_rets.resample("Y").apply(get_top_100)
top_daily_movements_by_year.head()

# %%
top_daily_movements_by_year.tail()

# %% [markdown]
# # 1-min movements

# %%
tau = 18

zscored_1min_rets = pma.get_zscored_returns(minutely_prices, "minutely", tau=tau)
zscored_1min_rets = zscored_1min_rets.abs()
top_1min_movements = get_top_100(zscored_1min_rets)

# %%
top_1min_movements.head()

# %%
top_1min_movements.plot(kind="bar")
plt.title(
    f"Largest price movements in a 1 min interval (in z-score space) for the {SYMBOL} symbol"
)
plt.show()

# %%
top_1min_movements_by_year = zscored_1min_rets.resample("Y").apply(get_top_100)
top_1min_movements_by_year.head()

# %% [markdown]
# # 5-min movements

# %%
tau = 18

zscored_5min_rets = pma.get_zscored_returns(five_min_prices, "minutely", tau=tau)
zscored_5min_rets = zscored_5min_rets.abs()
top_5min_movements = get_top_100(zscored_5min_rets)

# %%
top_5min_movements.head()

# %%
print(
    f"Top 100 of the 5-min price movements for {SYMBOL} occur at the following time:"
)
print(pd.Series(top_5min_movements.index).dt.time.value_counts())

# %%
top_5min_movements_by_year = zscored_5min_rets.resample("Y").apply(get_top_100)
top_5min_movements_by_year.head()

# %%
top_5min_movements_by_year.tail()

# %%
