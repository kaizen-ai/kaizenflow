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
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt

import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hprint as hprint
import defi.dao_cross.supply_demand as ddcrsede

# %%
hdbg.init_logger(verbosity=logging.DEBUG)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %% [markdown]
# # Pathological cases

# %% [markdown]
# ## Get orders for supply and demand curves.

# %%
supply_orders = ddcrsede.get_supply_orders1()
supply_orders

# %%
demand_orders = ddcrsede.get_demand_orders1()
demand_orders

# %% [markdown]
# ## Multiple intersection points at quantity Q'

# %%
type_ = "supply"
supply_curve = ddcrsede.get_curve(supply_orders, type_)
supply_curve

# %%
type_ = "demand"
demand_curve = ddcrsede.get_curve(demand_orders, type_)
demand_curve

# %%
plt.plot(*zip(*supply_curve))
plt.plot(*zip(*demand_curve))
plt.show()

# %% [markdown]
# ## Multiple intersection points at price P'

# %%
quantity_const = 10.0
supply_orders2 = ddcrsede.get_supply_orders1(quantity_const=quantity_const)
type_ = "supply"
supply_curve2 = ddcrsede.get_curve(supply_orders2, type_)
supply_curve2

# %%
plt.plot(*zip(*supply_curve2))
plt.plot(*zip(*demand_curve))
plt.show()

# %% [markdown]
# ## No intersection

# %%
quantity_const = 100.0
supply_orders3 = ddcrsede.get_supply_orders1(quantity_const=quantity_const)
type_ = "supply"
supply_curve3 = ddcrsede.get_curve(supply_orders3, type_)
supply_curve3

# %%
plt.plot(*zip(*supply_curve3))
plt.plot(*zip(*demand_curve))
plt.show()

# %%
