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
# # Imports

# %%
# %load_ext autoreload
# %autoreload 2

import numpy as np
import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt

# %%
from pylab import rcParams
from tqdm.autonotebook import tqdm

import core.signal_processing as sp
import vendors.kibot.utils as kut
import vendors.particle_one.price_movement_analysis as pma

# import vendors.particle_one.PartTask269_liquidity_analysis_utils as lau

sns.set()

rcParams["figure.figsize"] = (20, 5)

# %%
TAU = 2

# %% [markdown]
# # Load CME metadata

# %%
# Change this to library code from #269 once it is merged into master

# %%
_PRODUCT_SPECS_PATH = (
    "/data/prices/product_slate_export_with_contract_specs_20190905.csv"
)
product_list = pd.read_csv(_PRODUCT_SPECS_PATH)

# %%
product_list.head()

# %%
product_list["Product Group"].value_counts()

# %%
product_list.set_index("Product Group", inplace=True)

# %%
commodity_groups = ["Energy", "Agriculture", "Metals"]

# %%
commodity_symbols = {
    group: product_list.loc[group]["Globex"].values for group in commodity_groups
}

# %%
commodity_symbols

# %% [markdown]
# # Daily price movements

# %% [markdown]
# ## Load kibot commodity daily prices

# %%
daily_metadata = kut.read_metadata2()
daily_metadata.head(3)

# %%
len(daily_metadata["Symbol"])

# %%
daily_metadata["Symbol"].nunique()

# %%
len(commodity_symbols["Energy"])

# %%
energy_symbols_kibot = np.intersect1d(
    daily_metadata["Symbol"].values, commodity_symbols["Energy"]
)
energy_symbols_kibot

# %%
len(energy_symbols_kibot)

# %%
commodity_symbols_kibot = {
    group: np.intersect1d(
        daily_metadata["Symbol"].values, commodity_symbols[group]
    )
    for group in commodity_symbols.keys()
}

# %%
commodity_symbols_kibot

# %%
{
    group: len(commodity_symbols_kibot[group])
    for group in commodity_symbols_kibot.keys()
}

# %%
comm_list = []
for comm_group in commodity_symbols_kibot.values():
    comm_list.extend(list(comm_group))
comm_list[:5]

# %%
file_name = "/data/kibot/All_Futures_Continuous_Contracts_daily/%s.csv.gz"

daily_price_dict_df = kut.read_multiple_symbol_data(
    comm_list, file_name, nrows=None
)

daily_price_dict_df["CL"].tail(2)

# %% [markdown]
# ## Largest movements for a specific symbol

# %%
# There is a pma.get_top_movements_for_symbol() function that
# implements this code and the code below. I am not using it
# in this chapter to provide a clearer view of the algorithm.

# %%
symbol = "CL"

# %%
cl_prices = daily_price_dict_df[symbol]

# %%
cl_prices_diff = cl_prices["close"] - cl_prices["open"]

# %%
zscored_cl_prices_diff = sp.rolling_zscore(cl_prices_diff, TAU)
zscored_cl_prices_diff.head()

# %%
abs_zscored_cl_prices_diff = zscored_cl_prices_diff.abs()

# %%
abs_zscored_cl_prices_diff.max()

# %%
top_100_movements_cl = abs_zscored_cl_prices_diff.sort_values(
    ascending=False
).head(100)

# %%
top_100_movements_cl.plot(kind="bar")
ax = plt.gca()
xlabels = [item.get_text()[:10] for item in ax.get_xticklabels()]
ax.set_xticklabels(xlabels)
plt.title(
    f"Largest price movements in a single day (in z-score space) for {symbol} symbol"
)
plt.show()

# %%
top_100_movements_cl.index.year.value_counts(sort=False).plot(kind="bar")
plt.title("How many of the top-100 price movements occured during each year")
plt.show()

# %% [markdown]
# ## Largest movement for energy group

# %%
group = "Energy"

# %%
commodity_symbols_kibot[group]

# %%
zscored_diffs = []
for symbol in commodity_symbols_kibot[group]:
    zscored_diff = pma.get_zscored_prices_diff(daily_price_dict_df, symbol)
    zscored_diffs.append(zscored_diff)

# %%
zscored_diffs = pd.concat(zscored_diffs, axis=1)
zscored_diffs.head()

# %%
mean_zscored_diffs = zscored_diffs.mean(axis=1, skipna=True)

# %%
mean_zscored_diffs.head()

# %%
mean_zscored_diffs.tail()

# %%
mean_zscored_diffs.sort_values(ascending=False).head(100)

# %% [markdown]
# ## Largest movements for each group

# %%
top_100_movements_by_group = {
    group: pma.get_top_movements_by_group(
        daily_price_dict_df, commodity_symbols_kibot, group
    )
    for group in commodity_symbols_kibot.keys()
}

# %%
top_100_movements_by_group.keys()

# %%
top_100_movements_by_group["Energy"].head()

# %%
top_100_movements_by_group["Agriculture"].head()

# %%
top_100_movements_by_group["Metals"].head()

# %% [markdown]
# # 5-minute price movements

# %% [markdown]
# ## Load 1-minute prices

# %%
minutely_metadata = kut.read_metadata1()

# %%
minutely_metadata.head()

# %%
np.array_equal(
    minutely_metadata["Symbol"].values, minutely_metadata["Symbol"].values
)

# %%
file_name = "/data/kibot/All_Futures_Continuous_Contracts_1min/%s.csv.gz"

minutely_price_dict_df = kut.read_multiple_symbol_data(
    comm_list, file_name, nrows=None
)

minutely_price_dict_df["CL"].tail(2)

# %%
minutely_price_dict_df["CL"].head()

# %%
five_min_price_dict_df = {
    symbol: minutely_price_dict_df[symbol].resample("5Min").sum()
    for symbol in minutely_price_dict_df.keys()
}

# %% [markdown]
# ## Top movements for a symbol

# %%
symbol = "CL"

# %%
top_100_movements_cl_5_min = pma.get_top_movements_for_symbol(
    five_min_price_dict_df, symbol
)

# %%
top_100_movements_cl_5_min["CL"].head()

# %%
top_100_movements_cl_5_min.plot(kind="bar")
plt.title(
    f"Largest price movements in in a 5 min interval (in z-score space) for {symbol} symbol"
)
plt.show()

# %%
print(f"Top 100 of the price movements for {symbol} occur at the following time:")
print(pd.Series(top_100_movements_cl_5_min.index).dt.time.value_counts())

# %% [markdown]
# ## Largest movements for energy group

# %%
group = "Energy"

# %%
commodity_symbols_kibot[group]

# %%
pma.get_top_movements_by_group(
    five_min_price_dict_df, commodity_symbols_kibot, group
)

# %% [markdown]
# ## Largest movements for each group

# %%
top_100_5_min_movements_by_group = {
    group: pma.get_top_movements_by_group(
        five_min_price_dict_df, commodity_symbols_kibot, group
    )
    for group in tqdm(commodity_symbols_kibot.keys())
}

# %%
{
    group: head_prices_group.head()
    for group, head_prices_group in top_100_5_min_movements_by_group.items()
}

# %%
