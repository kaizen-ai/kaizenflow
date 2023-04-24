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
try:
    import pulp
except ImportError:
    # !sudo /bin/bash -c "(source /venv/bin/activate; pip install pulp)"
    import pulp
import defi.dao_cross.optimize as ddacropt

# %%
hdbg.init_logger(verbosity=logging.DEBUG)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %% [markdown]
# # Pathological cases

# %% [markdown]
# ## Get orders and set prices.

# %%
type_ = "supply"
supply_quantities = [40, 40, 30, 30, 20, 20]
supply_limit_prices = [100, 60, 40, 30, 20, 10]
#
supply_orders1 = ddcrsede.get_curve_orders(
    type_, supply_quantities, supply_limit_prices
)
supply_orders1

# %%
type_ = "demand"
demand_quantities = [10, 30, 20, 40, 50, 30]
demand_limit_prices = [110, 100, 80, 60, 40, 30]
#
demand_orders = ddcrsede.get_curve_orders(
    type_, demand_quantities, demand_limit_prices
)
demand_orders

# %%
prices = {"BTC": 2, "ETH": 1}

# %% [markdown]
# ## Multiple intersection points at quantity Q'

# %%
type_ = "supply"
supply_curve1 = ddcrsede.get_curve(supply_orders1, type_)
supply_curve1

# %%
type_ = "demand"
demand_curve = ddcrsede.get_curve(demand_orders, type_)
demand_curve

# %%
plt.plot(*zip(*supply_curve1))
plt.plot(*zip(*demand_curve))
plt.show()

# %% run_control={"marked": false}
all_orders1 = supply_orders1 + demand_orders
daocross_results1 = ddacropt.run_daocross_solver(all_orders1, prices)
display(daocross_results1)

# %% [markdown]
# ## Multiple intersection points at price P'

# %%
quantity_const = 10.0
supply_orders2 = ddcrsede.get_curve_orders(
    type_,
    supply_quantities,
    supply_limit_prices,
    quantity_const=quantity_const,
)
supply_orders2

# %%
type_ = "supply"
supply_curve2 = ddcrsede.get_curve(supply_orders2, type_)
supply_curve2

# %%
plt.plot(*zip(*supply_curve2))
plt.plot(*zip(*demand_curve))
plt.show()

# %%
all_orders2 = supply_orders2 + demand_orders
daocross_results2 = ddacropt.run_daocross_solver(all_orders2, prices)
display(daocross_results2)

# %% [markdown]
# ## No intersection, demand is higher

# %%
quantity_const = 100.0
supply_orders3 = ddcrsede.get_curve_orders(
    type_,
    supply_quantities,
    supply_limit_prices,
    quantity_const=quantity_const,
)
supply_orders3

# %%
type_ = "supply"
supply_curve3 = ddcrsede.get_curve(supply_orders3, type_)
supply_curve3

# %%
plt.plot(*zip(*supply_curve3))
plt.plot(*zip(*demand_curve))
plt.show()

# %%
all_orders3 = supply_orders3 + demand_orders
daocross_results3 = ddacropt.run_daocross_solver(all_orders3, prices)
display(daocross_results3)

# %% [markdown]
# # No intersection, demand is lower

# %%
# TODO(Dan): Implement

# %%
