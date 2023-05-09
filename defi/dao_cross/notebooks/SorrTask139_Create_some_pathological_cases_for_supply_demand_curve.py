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
import defi.dao_cross.order as ddacrord

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
supply_quantities = [1, 1, 1, 1]
supply_limit_prices = [1.5, 2, 3, 3.5]
#
supply_orders1 = ddcrsede.get_curve_orders(
    type_, supply_quantities, supply_limit_prices
)
ddacrord.convert_orders_to_dataframe(supply_orders1)

# %%
type_ = "demand"
demand_quantities = [1, 1, 1, 1]
demand_limit_prices = [3.5, 2.5, 2, 1.5]
#
demand_orders1 = ddcrsede.get_curve_orders(
    type_, demand_quantities, demand_limit_prices
)
ddacrord.convert_orders_to_dataframe(demand_orders1)

# %%
prices = {"BTC": 1, "ETH": 2}

# %% [markdown]
# ## Multiple intersection points at quantity Q'

# %%
type_ = "supply"
supply_curve1 = ddcrsede.get_curve_dots(supply_orders1, type_)
supply_curve1

# %% run_control={"marked": false}
type_ = "demand"
demand_curve1 = ddcrsede.get_curve_dots(demand_orders1, type_)
demand_curve1

# %%
plt.plot(*zip(*supply_curve1))
plt.plot(*zip(*demand_curve1))
plt.show()

# %% run_control={"marked": false}
all_orders1 = supply_orders1 + demand_orders1
daocross_results1 = ddacropt.run_daocross_solver(all_orders1, prices)
display(daocross_results1)

# %%
daoswap_results1 = ddacropt.run_daoswap_solver(all_orders1)
display(daoswap_results1)

# %% [markdown]
# ## Multiple intersection points at price P'

# %%
quantity_const = 1.0
type_ = "supply"
supply_orders2 = ddcrsede.get_curve_orders(
    type_,
    supply_quantities,
    supply_limit_prices,
    quantity_const=quantity_const,
)
ddacrord.convert_orders_to_dataframe(supply_orders2)

# %%
type_ = "supply"
supply_curve2 = ddcrsede.get_curve_dots(supply_orders2, type_)
supply_curve2

# %%
plt.plot(*zip(*supply_curve2))
plt.plot(*zip(*demand_curve1))
plt.show()

# %%
all_orders2 = supply_orders2 + demand_orders1
daocross_results2 = ddacropt.run_daocross_solver(all_orders2, prices)
display(daocross_results2)

# %%
daoswap_results2 = ddacropt.run_daoswap_solver(all_orders2)
display(daoswap_results2)

# %% [markdown]
# ## No intersection

# %%
limit_price_const = 5.0
supply_orders3 = ddcrsede.get_curve_orders(
    type_,
    supply_quantities,
    supply_limit_prices,
    limit_price_const=limit_price_const,
)
ddacrord.convert_orders_to_dataframe(supply_orders3)

# %%
type_ = "supply"
supply_curve3 = ddcrsede.get_curve_dots(supply_orders3, type_)
supply_curve3

# %%
plt.plot(*zip(*supply_curve3))
plt.plot(*zip(*demand_curve1))
plt.show()

# %%
all_orders3 = supply_orders3 + demand_orders1
daocross_results3 = ddacropt.run_daocross_solver(all_orders3, prices)
display(daocross_results3)

# %%
daoswap_results3 = ddacropt.run_daoswap_solver(all_orders3)
display(daoswap_results3)

# %%
