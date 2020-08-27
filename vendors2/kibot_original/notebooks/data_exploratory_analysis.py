# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.5.2
#   kernelspec:
#     display_name: Python [conda env:.conda-p1_develop] *
#     language: python
#     name: conda-env-.conda-p1_develop-py
# ---

# %% [markdown]
# # Description
# - This notebook shows how to use the Kibot API

# %% [markdown]
# ## Import

# %%
# %load_ext autoreload
# %autoreload 2
import logging

import pandas as pd
import seaborn as sns

import core.config as cfg
import core.explore as exp
import core.finance as fin
import core.signal_processing as sigp
import helpers.dbg as dbg
import helpers.env as env
import helpers.printing as pri
import vendors2.kibot.utils as kut

# %%
print(env.get_system_signature())

pri.config_notebook()

# dbg.init_logger(verbosity=logging.DEBUG)
dbg.init_logger(verbosity=logging.INFO)
# dbg.test_logger()

_LOG = logging.getLogger(__name__)

# %% [markdown]
# # Metadata

# %% [markdown]
# ## Read metadata

# %%
kmd = kut.KibotMetadata()
metadata = kmd.get_metadata()
metadata

# %%
metadata.sort_index()

# %% [markdown]
# ## Read misc metadata

# %%
df1 = kut.read_1min_contract_metadata()
df1.head(3)

# %%
df2 = kut.read_daily_contract_metadata()
df2.head(3)

# %%
df3 = kut.read_tickbidask_contract_metadata()
df3.head(3)

# %%
df4 = kut.read_continuous_contract_metadata()
print(df4.head(3))

print(df4["Exchange"].unique())

# %%
df4.dropna(how="all")

# %% [markdown]
# ## Explore metadata

# %%
mask = ["GAS" in d or "OIL" in d for d in df4["Description"].values]
print(sum(mask))
print(df4[mask].drop(["SymbolBase", "Size(MB)"], axis=1))

# %% [markdown]
# # Price data

# %% [markdown]
# ## Read continuous daily prices for single futures

# %%
s = "CL"
# nrows = None
nrows = 10000
df = kut.read_data("D", "continuous", s, nrows=nrows)
df.head(3)

# %% [markdown]
# ## Read continuous 1-min prices for single futures

# %%
s = "CL"
# nrows = None
nrows = 10000
df = kut.read_data("T", "continuous", s, nrows=nrows)
df.head(3)

# %%
## Read continuous 1-min prices for multiple futures

# %% [markdown]
# ## Read continuous daily prices for multiple futures

# %%
symbols = tuple("CL NG RB BZ".split())
nrows = 10000

daily_price_dict_df = kut.read_data("D", "continuous", symbols, nrows=nrows)

daily_price_dict_df["CL"].head(3)

# %% [markdown]
# ## Read continuous 1-min prices for multiple futures

# %%
symbols = tuple("CL NG RB BZ".split())
nrows = 10000

daily_price_dict_df = kut.read_data("D", "continuous", symbols, nrows=nrows)

daily_price_dict_df["CL"].head(3)

# %% [markdown]
# ## Read data through config API

# %%
config = cfg.Config.from_env()

if config is None:
    config = cfg.Config()
    config_tmp = config.add_subconfig("read_data")
    # Use the data from S3.
    file_name = hs3.get_path() + "/kibot/All_Futures_Contracts_1min/ES.csv.gz"
    config_tmp["file_name"] = file_name
    config_tmp["nrows"] = 100000

_LOG.info(config)

# %%
def read_data_from_config(config):
    _LOG.info("Reading data ...")
    config.check_params(["file_name"])
    return kut._read_data(config["file_name"], config.get("nrows", None))


df = read_data_from_config(config["read_data"])

_LOG.info("df.shape=%s", df.shape)
_LOG.info("datetimes=[%s, %s]", df.index[0], df.index[-1])
_LOG.info("df=\n%s", df.head(3))

# %% [markdown]
# ## Read raw data directly from S3

# %%
s = "CL"
file_name = (
    hs3.get_path() + "/kibot/All_Futures_Continuous_Contracts_1min/%s.csv.gz" % s
)
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
symbols = tuple("CL NG RB BZ".split())
nrows = 100000
min_price_dict_df = kut.read_data(
    "T", "continuous", symbols, ext="csv", nrows=nrows
)
_LOG.info("keys=%s", min_price_dict_df.keys())
min_price_dict_df["CL"].tail(3)

# %% [markdown]
# ### Compute returns ret_0

# %%
def compute_ret_0_from_multiple_1min_prices(price_dict_df, mode):
    dbg.dassert_isinstance(price_dict_df, dict)
    rets = []
    for s, price_df in price_dict_df.items():
        _LOG.debug("Processing s=%s", s)
        rets_tmp = fin.compute_ret_0(price_df["open"], mode)
        rets_tmp = pd.DataFrame(rets_tmp)
        rets_tmp.columns = ["%s_ret_0" % s]
        rets.append(rets_tmp)
    rets = pd.concat(rets, sort=True, axis=1)
    return rets


mode = "pct_change"
min_rets = compute_ret_0_from_multiple_1min_prices(min_price_dict_df, mode)


min_rets.head(3)

# %%
sigp.resample(min_rets.fillna(0.0), rule="1D").sum().cumsum().plot()

# %% [markdown]
# ### Resample to 1min

# %%
# Resample to 1min.
_LOG.info("## Before resampling")
exp.report_zero_nan_inf_stats(min_rets)

# %%
exp.plot_non_na_cols(sigp.resample(min_rets, rule="1D").sum())

# %%
min_rets = fin.resample_1min(min_rets, skip_weekends=False)

_LOG.info("## After resampling")
exp.report_zero_nan_inf_stats(min_rets)

min_rets.fillna(0.0, inplace=True)

# %% [markdown]
# ### z-scoring

# %%
zscore_com = 28
min_zrets = fin.zscore(
    min_rets, com=zscore_com, demean=False, standardize=True, delay=1
)
min_zrets.columns = [c.replace("ret_", "zret_") for c in min_zrets.columns]
min_zrets.dropna().head(3)

# %%
sigp.resample(min_zrets.fillna(0.0), rule="1D").sum().cumsum().plot()

# %%
annot = True
stocks_corr = min_rets.dropna().corr()

sns.clustermap(stocks_corr, annot=annot)

# %% [markdown]
# ## Daily for single futures

# %% [markdown]
# ## Daily for multiple futures
