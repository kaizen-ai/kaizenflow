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
# # Get aggregated supply / demand

# %%
type_ = "supply"
alpha = 2.0
beta = 10.0
n_orders = 10
agg_supply_curve = ddcrsede.get_supply_demand_aggregated_curve(
    type_, alpha, beta, n_orders
)
agg_supply_curve

# %%
type_ = "demand"
alpha = -2.0
beta = 210.0
n_orders = 10
agg_demand_curve = ddcrsede.get_supply_demand_aggregated_curve(
    type_, alpha, beta, n_orders
)
agg_demand_curve

# %%
plt.plot(agg_supply_curve)
plt.plot(agg_demand_curve)
plt.show()

# %%
agg_supply_orders = ddcrsede.convert_aggregated_curve_to_limit_orders(agg_supply_curve)
agg_supply_orders_df = ddacrord.convert_orders_to_dataframe(agg_supply_orders)
agg_supply_orders_df

# %%
agg_demand_orders = ddcrsede.convert_aggregated_curve_to_limit_orders(agg_demand_curve)
agg_demand_orders_df = ddacrord.convert_orders_to_dataframe(agg_demand_orders)
agg_demand_orders_df

# %% [markdown]
# # Implement DaoCross and DaoSwap

# %%
agg_all_orders = agg_supply_orders + agg_demand_orders
prices = {"BTC": 1, "ETH": 50}

# %%
daocross_results = ddacropt.run_daocross_solver(agg_all_orders, prices)
display(daocross_results)

# %%
daoswap_results = ddacropt.run_daoswap_solver(agg_all_orders)
display(daoswap_results)

# %% [markdown]
# # Sanity check with discrete supply / demand

# %%
type_ = "supply"
descrete_supply_curve = ddcrsede.get_supply_demand_discrete_curve(
    type_, agg_supply_orders_dfagg_supply_orders_df
)
descrete_supply_curve

# %%
type_ = "demand"
descrete_demand_curve = ddcrsede.get_supply_demand_discrete_curve(
    type_, agg_demand_orders_df
)
descrete_demand_curve

# %%
plt.plot(descrete_supply_curve)
plt.plot(descrete_demand_curve)
plt.show()

# %%
