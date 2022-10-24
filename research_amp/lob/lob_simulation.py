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

# %%
# Assume that each person has a limit order distribution given by a N(a, b) for selling and buying

# N = number of actors
# Bid = B_mean, B_std
# Ask = A_mean, A_std

# The orders are inserted in a queue and matched

# %%
import numpy as np
import pandas as pd
import scipy as scipy

# %%
# %load_ext autoreload
# %autoreload 2

# %%
import lib

# %%
print(ask_rv)
print(bid_rv)

# %%
ob = lib.get_data()

display(ob)

ob.plot.hist()

# %%
sd = lib.get_supply_demand_curve(ob)

display(sd)

sd.plot()

# %%
sd.index[4]

# %%
eq_idx = lib.find_equilibrium(sd)
#eq_quantity = (sd.iloc[eq_idx].index.values[0] + sd.iloc[eq_idx + 1].index.values[0]) / 2
print(sd.iloc[eq_idx].index)
eq_quantity1 = sd.index.values[eq_idx]
eq_quantity2 = sd.index.values[eq_idx + 1]
#print(eq_quantity1, eq_quantity2)
eq_quantity = np.mean([eq_quantity1, eq_quantity2])
print(eq_idx, eq_quantity)
ax = sd.plot()
ymin, ymax = ax.get_ylim()
ax.vlines(eq_quantity, ymin=ymin, ymax=ymax, color="r")

# %%
excess = df["supply"] - df["demand"]
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
a
