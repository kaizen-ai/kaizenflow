# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.2'
#       jupytext_version: 1.2.4
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %% [markdown]
# ## Import

# %%

# %%
# %load_ext autoreload
# %autoreload 2
import logging

import seaborn as sns

import core.config as cfg
import core.finance as fin
import helpers.dbg as dbg
import helpers.env as env
import helpers.printing as pri
import helpers.s3 as hs3
import vendors.kibot.utils as kut

# %%
print(env.get_system_signature())

pri.config_notebook()

# TODO(gp): Changing level during the notebook execution doesn't work. Fix it.
# dbg.init_logger(verb=logging.DEBUG)
dbg.init_logger(verb=logging.INFO)
# dbg.test_logger()

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

# %% [markdown]
# # Read data

# %%
config = cfg.Config.from_env()

if config is None:
    config = cfg.Config()
    config_tmp = config.add_subconfig("read_data")
    # config_tmp["nrows"] = 100000
    config_tmp["nrows"] = None
    #
    config["zscore_com"] = 28

print(config)

# %% [markdown]
# # Prices

# %% [markdown]
# ## Read daily prices

# %%
symbols = "CL NG RB BZ".split()
file_name = os.path.join(
    hs3.get_path(), "kibot/All_Futures_Continuous_Contracts_daily/%s.csv.gz"
)

daily_price_dict_df = kut.read_multiple_symbol_data(
    symbols, file_name, nrows=config["read_data"]["nrows"]
)

daily_price_dict_df["CL"].tail(2)

# %%
col_name = "close"
mode = "pct_change"
daily_rets = kut.compute_ret_0_from_multiple_daily_prices(
    daily_price_dict_df, col_name, mode
)

daily_rets.head(3)

# %%
daily_rets = daily_rets["2008-01-01":]
daily_rets.fillna(0.0).cumsum().plot()

# %%
daily_zrets = fin.zscore(
    daily_rets, com=config["zscore_com"], demean=False, standardize=True, delay=1
)
daily_zrets.columns = [c.replace("ret_", "zret_") for c in daily_zrets.columns]
daily_zrets.dropna().head(3)

# %%
daily_zrets = daily_zrets["2008-01-01":]

daily_zrets.fillna(0.0).cumsum().plot()

# %%
annot = True
stocks_corr = daily_zrets.corr()

sns.clustermap(stocks_corr, annot=annot)

# %%
print(daily_zrets.head(2))
daily_rets.to_csv("oil_daily_zrets.csv")

# %% [markdown]
# ## Read 1 min prices

# %%
symbols = "CL NG RB BZ".split()
file_name = os.path.join(
    hs3.get_path(), "kibot/All_Futures_Continuous_Contracts_1min/%s.csv.gz"
)
min_price_dict_df = kut.read_multiple_symbol_data(
    symbols, file_name, nrows=config["read_data"]["nrows"]
)

min_price_dict_df["CL"].tail(2)

# %%
min_price_dict_df.keys()

# %%
mode = "pct_change"
min_rets = kut.compute_ret_0_from_multiple_1min_prices(min_price_dict_df, mode)

min_rets.head(3)

# %%
min_rets.fillna(0.0).resample("1D").sum().cumsum().plot()

# %%
min_zrets = fin.zscore(
    min_rets, com=config["zscore_com"], demean=False, standardize=True, delay=1
)
min_zrets.columns = [c.replace("ret_", "zret_") for c in min_zrets.columns]
min_zrets.dropna().head(3)

# %%
min_zrets = min_rets["2008-01-01":]

min_zrets.fillna(0.0).resample("1d").sum().cumsum().plot()

# %%
annot = True
stocks_corr = min_rets.corr()

sns.clustermap(stocks_corr, annot=annot)

# %%
print(min_zrets.head(2))
min_zrets.to_csv("oil_1min_zrets.csv")
