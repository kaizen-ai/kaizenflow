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

import numpy as np
import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt

# %%
from pylab import rcParams

import core.config as cfg
import helpers.dbg as dbg
import helpers.env as env
import helpers.printing as pri

# %%
import helpers.s3 as hs3
import vendors.kibot.PartTask269_liquidity_analysis_utils as lau
import vendors.kibot.utils as kut

sns.set()


rcParams["figure.figsize"] = (20, 5)

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
# # Load product specs


# %%
# TODO (Julia): After PartTask268_PRICE_Download_metadata_from_CME
# is merged into master, replace this with a reader
_PRODUCT_SPECS_PATH = os.path.join(
    hs3.get_path(), "cme/product_slate_export_with_contract_specs_20190905.csv"
)
product_specs = pd.read_csv(_PRODUCT_SPECS_PATH)

# %%
product_specs.head()

# %%
product_specs.info()

# %% [markdown]
# # Explore metadata

# %%
df4["Exchange"].value_counts()

# %%
df3["Exchange"].value_counts()

# %% [markdown]
# Kibot only has the CME group futures.

# %%
product_specs["Globex"].head()

# %%
# daily_futures_w_ext = os.listdir
#    "/data/kibot/All_Futures_Continuous_Contracts_daily/"
# )

file_path = os.path.join(
    hs3.get_path(), "kibot/All_Futures_Continuous_Contracts_daily"
)
daily_futures_w_ext = hs3.ls(file_path)

# %%
daily_futures_w_ext[:5]

# %%
daily_futures = list(map(lambda x: x[:-7], daily_futures_w_ext))
daily_futures[:5]

# %%
len(set(daily_futures)), df3["SymbolBase"].nunique()

# %%
np.setdiff1d(df3["SymbolBase"].dropna().values, daily_futures)

# %%
product_specs["Globex"].nunique()

# %%
np.intersect1d(
    product_specs["Globex"].dropna().unique(), df3["SymbolBase"].dropna().values
)

# %%
np.intersect1d(
    product_specs["Globex"].dropna().unique(), df3["SymbolBase"].dropna().values
).shape

# %%
np.intersect1d(
    product_specs["Globex"].dropna().unique(), df2["Symbol"].dropna().values
).shape

# %%
np.intersect1d(
    product_specs["Globex"].dropna().unique(), df1["Symbol"].dropna().values
).shape

# %%
product_specs[product_specs["Globex"].isna()]

# %%

# %%

# %%

# %%
mask = ["GAS" in d or "OIL" in d for d in df4["Description"].astype(str)]
print(sum(mask))
print(df4[mask].drop(["SymbolBase", "Size(MB)"], axis=1))

# %%
df4[mask]["Symbol"].values

# %% [markdown]
# # Read config

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
all_symbols = [
    futures.replace(".csv.gz", "")
    for futures in os.listdir(
        "/data/kibot/All_Futures_Continuous_Contracts_daily"
    )
]

# %%
symbols = df4[mask]["Symbol"].values
symbols

# %%
file_name = "/data/kibot/All_Futures_Continuous_Contracts_daily/%s.csv.gz"

daily_price_dict_df = kut.read_multiple_symbol_data(
    symbols, file_name, nrows=config["read_data"]["nrows"]
)

daily_price_dict_df["CL"].tail(2)

# %% [markdown]
# # Top futures by volume

# %% [markdown]
# ## Sum volume

# %%
daily_vol = lau.get_sum_daily_prices(daily_price_dict_df, price_col=lau.KIBOT_VOL)
daily_vol.sort_values("sum_vol", ascending=False)

# %% [markdown]
# ## Mean volume

# %%
mean_vol = lau.get_mean_daily_prices(daily_price_dict_df, price_col=lau.KIBOT_VOL)
mean_vol.sort_values("mean_vol", ascending=False)

# %% [markdown]
# # Study volume

# %%
symbol = "CL"

# %%
vs = lau.PricesStudy(lau.read_kibot_prices, symbol, lau.KIBOT_VOL, n_rows=None)

# %%
vs.execute()

# %% [markdown]
# ## How is the volume related to the open interest from the metadata?

# %%
product_specs.head()

# %%
product_specs[product_specs["Globex"] == symbol]["Open Interest"].values

# %%
product_specs[product_specs["Globex"] == symbol]["Volume"].values

# %%
vs.daily_prices[lau.KIBOT_VOL].max()

# %%
vs.minutely_prices[lau.KIBOT_VOL].max()

# %% [markdown]
# # CME mapping

# %% [markdown]
# ## Groups overview

# %%
pc = lau.ProductSpecs()

# %%
pc.product_specs.info()

# %%
pc.product_specs["Product Group"].value_counts().plot(kind="bar", rot=0)
plt.title("Number of futures for each product group in CME")
plt.show()

# %%
pc.product_specs["Sub Group"].value_counts().plot(kind="bar")
plt.xticks(ha="right", rotation=30, rotation_mode="anchor")
plt.title("Number of futures for each sub group in CME")
plt.show()

# %%
pc.product_specs["Category"].astype(str).value_counts()

# %%
pc.product_specs["Sub Category"].astype(str).value_counts()

# %% [markdown]
# ## By symbol

# %%
pc.get_metadata_symbol(symbol)

# %%
pc.get_product_group(symbol)

# %%
pc.get_trading_hours(symbol)

# %% [markdown]
# ## For product group

# %%
energy_symbols = pc.get_symbols_product_group("Energy")
energy_symbols[:4]

# %%
np.intersect1d(energy_symbols, daily_futures)

# %%
np.intersect1d(energy_symbols, daily_futures).shape

# %%
