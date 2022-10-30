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

import numpy as np
import pandas as pd
import scipy as scipy
import lob_lib as llib

# %% [markdown]
# # Generate the data

# %%
n_samples = 10
bid_asks_raw = llib.get_data(n_samples)
#
display(bid_asks_raw)
#
bid_asks_raw.plot.hist()

# %% [markdown]
# # Convert raw orders data into `supply-demand` state

# %%
supply_demand = llib.get_supply_demand_curve(bid_asks_raw)
#
display(supply_demand.head(3))
#
supply_demand.plot()

# %% [markdown]
# # Find the equilibrium price and quantity

# %%
eq_price, eq_quantity = llib.find_equilibrium(supply_demand)

# %%

# %%

# %%

# %%

# %%

# %%
sd = supply_demand.copy()

# %%
excess = sd["supply"] - sd["demand"]
excess.plot()
zero_crossings = np.where(np.diff(np.sign(excess)))
idx = zero_crossings[0]
print(idx)
excess.iloc[idx]

# %%
excess
idx = np.where(np.diff(np.sign(excess)) == 2)[0]
excess.iloc[idx + 1]

# %%

# %%

# %%

# %%
