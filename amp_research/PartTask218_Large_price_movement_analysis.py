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

import logging
import os

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt

# %%
from tqdm.autonotebook import tqdm

import amp_research.price_movement_analysis as pma
import helpers.dbg as dbg
import helpers.env as env
import helpers.printing as pri
import helpers.s3 as hs3
import vendors.cme.read as cmer
import vendors.kibot.utils as kut

# %%
print(env.get_system_signature())

pri.config_notebook()

dbg.init_logger(verb=logging.INFO)

_LOG = logging.getLogger(__name__)

# %% [markdown]
# # Load CME metadata

# %%
product_list = cmer.read_product_specs()

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
s3_path = hs3.get_path()
kibot_path = os.path.join(
    s3_path, "kibot/All_Futures_Continuous_Contracts_daily/%s.csv.gz"
)

daily_price_dict_df = kut.read_multiple_symbol_data(
    comm_list, kibot_path, nrows=None
)

daily_price_dict_df["CL"].tail(2)

# %% [markdown]
# ## Largest movements for a specific symbol

# %%
symbol = "CL"

# %%
top_daily_movements_cl = pma.get_top_movements_for_symbol(
    daily_price_dict_df, symbol, "daily"
)
top_daily_movements_cl.head()

# %%
top_daily_movements_cl.index.year.value_counts(sort=False).plot(kind="bar")
plt.title("How many of the top-100 price movements occured during each year")
plt.show()

# %% [markdown]
# ## Largest movement for energy group

# %%
group = "Energy"

# %%
commodity_symbols_kibot[group]

# %%
pma.get_top_movements_by_group(
    daily_price_dict_df, commodity_symbols_kibot, group, "daily"
)

# %% [markdown]
# ## Largest movements for each group

# %%
top_100_daily_movements_by_group = {
    group: pma.get_top_movements_by_group(
        daily_price_dict_df, commodity_symbols_kibot, group, "daily"
    )
    for group in tqdm(commodity_symbols_kibot.keys())
}

# %%
{
    group: head_prices_group.head(15)
    for group, head_prices_group in top_100_daily_movements_by_group.items()
}

# %% [markdown]
# # 1-minute price movements

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
s3_path = hs3.get_path()
kibot_path = os.path.join(
    s3_path, "kibot/All_Futures_Continuous_Contracts_1min/%s.csv.gz"
)

minutely_price_dict_df = kut.read_multiple_symbol_data(
    comm_list, kibot_path, nrows=None
)

daily_price_dict_df["CL"].tail(2)

# %%
minutely_price_dict_df["CL"].head()

# %%
# five_min_price_dict_df = {
#     symbol: minutely_price_dict_df[symbol].resample("5Min").sum()
#     for symbol in minutely_price_dict_df.keys()
# }

# %% [markdown]
# ## Top movements for a symbol

# %%
symbol = "CL"

# %%
top_100_movements_cl_1_min = pma.get_top_movements_for_symbol(
    minutely_price_dict_df, symbol, "minutely"
)

# %%
top_100_movements_cl_1_min.head()

# %%
# top_100_movements_cl_5_min = pma.get_top_movements_for_symbol(
#     five_min_price_dict_df, symbol
# )

# %%
# top_100_movements_cl_5_min.head()

# %%
top_100_movements_cl_1_min.plot(kind="bar")
plt.title(
    f"Largest price movements in a 1 min interval (in z-score space) for {symbol} symbol"
)
plt.show()

# %%
print(f"Top 100 of the price movements for {symbol} occur at the following time:")
print(pd.Series(top_100_movements_cl_1_min.index).dt.time.value_counts())

# %% [markdown]
# ## Largest movements for energy group

# %%
group = "Energy"

# %%
commodity_symbols_kibot[group]

# %%
pma.get_top_movements_by_group(
    minutely_price_dict_df, commodity_symbols_kibot, group, "minutely"
)

# %% [markdown]
# ## Largest movements for each group

# %%
top_100_1_min_movements_by_group = {
    group: pma.get_top_movements_by_group(
        minutely_price_dict_df, commodity_symbols_kibot, group, "minutely"
    )
    for group in tqdm(commodity_symbols_kibot.keys())
}

# %%
{
    group: head_prices_group.head()
    for group, head_prices_group in top_100_1_min_movements_by_group.items()
}

# %%
