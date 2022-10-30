# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.14.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# - Assume that each person has a limit order distribution given by a N(a, b) for selling and buying
#
#    - N = number of actors
#    - Bid = B_mean, B_std, equals to the demand on the market (orders to purchase asset)
#    - Ask = A_mean, A_std, equals to the supply on the market (orders to sell asset)
#
# - The orders are inserted in a queue and matched

# %% [markdown]
# # Imports

# %%
# %load_ext autoreload
# %autoreload 2

import research_amp.lob.lob_lib as ralololi
import numpy as np
import pandas as pd

# %% [markdown]
# # Generate the data

# %%
n_samples = 10
bid_asks_raw = ralololi.get_data(n_samples)
#
display(bid_asks_raw)
#
bid_asks_raw.plot.hist()

# %% [markdown]
# # Convert raw orders data into `supply-demand` state

# %%
supply_demand = ralololi.get_supply_demand_curve(bid_asks_raw)
#
display(supply_demand.head(3))
#
supply_demand.plot()

# %% [markdown]
# # Find the equilibrium price and quantity

# %%
eq_price, eq_quantity = ralololi.find_equilibrium(supply_demand)

# %% [markdown]
# # Check the supply-demand imbalances

# %%
excess = supply_demand["supply"] - supply_demand["demand"]
excess.plot()
zero_crossings_idx = np.where(np.diff(np.sign(excess)) == 2)[0]
print(zero_crossings_idx)
excess.iloc[zero_crossings_idx[0] : zero_crossings_idx[0] + 2]

# %% [markdown]
# # MC simulation for equilibrium

# %%
eq_df = pd.DataFrame()
for i in range(1, 500):
    ba_raw = ralololi.get_data(n_samples)
    sd = ralololi.get_supply_demand_curve(ba_raw)
    eq_p, eq_q = ralololi.find_equilibrium(sd, print_graph=False)
    eq_df.loc[i, "eq_price"] = eq_p
    eq_df.loc[i, "eq_quantity"] = eq_q


# %%
eq_df["eq_price"].hist(bins=30)

# %%
eq_df["eq_quantity"].hist(bins=20)
