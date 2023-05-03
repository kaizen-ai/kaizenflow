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
# %load_ext autoreload
# %autoreload 2

import logging
import matplotlib as mpl
import matplotlib.pyplot as plt

import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hprint as hprint
import defi.dao_cross.supply_demand as ddcrsede
import defi.dao_cross.order as ddacrord

# %%
hdbg.init_logger(verbosity=logging.DEBUG)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %% [markdown]
# # Get linear supply / demand orders

# %%
type_ = "supply"
alpha = 2.0
beta = 10.0
n_orders = 10
linear_supply_orders = ddcrsede.convert_aggregated_curve_to_limit_orders(type_, alpha, beta, n_orders)
ddacrord.convert_orders_to_dataframe(linear_supply_orders)

# %%
type_ = "demand"
alpha = -2.0
beta = 210.0
n_orders = 10
linear_demand_orders = ddcrsede.convert_aggregated_curve_to_limit_orders(type_, alpha, beta, n_orders)
ddacrord.convert_orders_to_dataframe(linear_demand_orders)

# %% [markdown]
# # Get curves

# %%
type_ = "supply"
supply_curve = ddcrsede.get_supply_demand_curve(type_, linear_supply_orders)
supply_curve

# %%
type_ = "demand"
demand_curve = ddcrsede.get_supply_demand_curve(type_, linear_demand_orders)
demand_curve

# %%
plt.plot(*zip(*supply_curve))
plt.plot(*zip(*demand_curve))
plt.show()

# %%
