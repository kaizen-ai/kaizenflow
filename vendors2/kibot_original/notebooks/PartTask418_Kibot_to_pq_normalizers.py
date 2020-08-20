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
# ## Import

# %%
# %load_ext autoreload
# %autoreload 2

import logging
import os

import pandas as pd

import helpers.dbg as dbg
import helpers.env as env
import helpers.printing as pri
import helpers.s3 as hs3
import vendors2.kibot.utils as kut

# %%
print(env.get_system_signature())

pri.config_notebook()

# dbg.init_logger(verbosity=logging.DEBUG)
dbg.init_logger(verbosity=logging.INFO)
# dbg.test_logger()

_LOG = logging.getLogger(__name__)

# %% [markdown]
# # List s3 files

# %%
# sys.maxsize is larger than Amazon's max int

# %%
s3_path = hs3.get_path()

# %%
kibot_dir = os.path.join(s3_path, "kibot")
kibot_dir

# %%
subdirs = hs3.listdir(s3_kibot, mode="non-recursive")
subdirs

# %%
kibot_subdir = os.path.join(kibot_dir, list(subdirs)[0])
kibot_subdir

# %% [markdown]
# # All_Futures_Continuous_Contracts_1min

# %%
symbol = "CL"
# nrows = None
nrows = 10
s3_path = hs3.get_path()
file_name = os.path.join(
    hs3.get_path(),
    "kibot/All_Futures_Continuous_Contracts_1min/%s.csv.gz" % symbol,
)
df = kut.read_data("T", "continuous", symbol, nrows=nrows)
df.head(3)

# %%
df = pd.read_csv(file_name, header=None)

# %%
df.head()

# %%
df[0] = pd.to_datetime(df[0] + " " + df[1], format="%m/%d/%Y %H:%M")

# %%
df.drop(columns=[1], inplace=True)

# %%
df.head()

# %%
df.columns = "datetime open high low close vol".split()
df.set_index("datetime", drop=True, inplace=True)
_LOG.debug("Add columns")
df["time"] = [d.time() for d in df.index]

# %%
df.head()

# %% [markdown]
# # All_Futures_Continuous_Contracts_daily

# %%
symbol = "CL"
# nrows = None
nrows = 10
s3_path = hs3.get_path()
file_name = os.path.join(
    hs3.get_path(),
    "kibot/All_Futures_Continuous_Contracts_daily/%s.csv.gz" % symbol,
)
df = kut.read_data("D", "continuous", symbol, nrows=nrows)
df.head(3)

# %%
df = pd.read_csv(file_name, header=None)

# %%
df.head()

# %%
df[0] = pd.to_datetime(df[0], format="%m/%d/%Y")

# %%
df.columns = "date open high low close vol".split()
df.set_index("date", drop=True, inplace=True)
# TODO(GP): Should this be renamed to datetime as described
# in kibot/utils.py L56?

# %%
df.head()

# %% [markdown]
# # All_Futures_Contracts_1min

# %%
symbol = "CL"
nrows = 10
s3_path = hs3.get_path()
file_name = os.path.join(
    hs3.get_path(), "kibot/All_Futures_Contracts_1min/%s.csv.gz" % symbol
)
df = kut.read_data("T", "continuous", symbol, nrows=nrows)
df.head(3)

# %%
df = pd.read_csv(file_name, header=None)

# %%
df.head()

# %% [markdown]
# The same as continuous contracts 1 min

# %% [markdown]
# # All_Futures_Continuous_Contracts_tick

# %%
symbol = "AD"
nrows = 10
s3_path = hs3.get_path()
file_name = "kibot/All_Futures_Continuous_Contracts_tick/%s.csv.gz" % symbol
file_name in kibot_files

# %%
file_name = os.path.join(hs3.get_path(), file_name)
print(file_name)

# %%
df = pd.read_csv(file_name, header=None, nrows=nrows)
df.head(3)

# %%
