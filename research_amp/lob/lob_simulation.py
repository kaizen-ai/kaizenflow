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

import random

import numpy as np
import pandas as pd

import research_amp.lob.lob_lib as ralololi

# %% [markdown]
# # Generate the data

# %%
n_samples = 10
bid_asks_raw = ralololi.get_data(n_samples)
#
display(bid_asks_raw)
bid_asks_raw.plot.hist()

# %% [markdown]
# # Convert raw orders data into `supply-demand` state

# %%
# TODO(Max): Invert the axes (place `price` to Y-axis).
supply_demand = ralololi.get_supply_demand_curve(bid_asks_raw)
#
display(supply_demand.head(3))
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

# %% [markdown]
# # Calculate the surplus

# %% [markdown]
# The total surplus is equal to `consumer surplus` + `producer surplus`:
#
# ![image.png](attachment:image.png)
#
# If we go back to our case, e.g., consumer surplus will be the following area:
# ![image-3.png](attachment:image-3.png)

# %% [markdown]
# ## Consumer surplus

# %%
# The calculations in this case will be the following.
cons_surplus = supply_demand.copy()
# Isolate the consumer surplus part.
cons_surplus = cons_surplus[cons_surplus.index > eq_price]["demand"]
cons_surplus = cons_surplus.reset_index().set_index("demand")
cons_surplus = cons_surplus.sort_index()
cons_surplus = cons_surplus[cons_surplus.index.notnull()]
# cons_surplus.plot()

# %% [markdown]
# ![image.png](attachment:image.png)

# %%
# The area below the line equals to consumer surplus.
cons_surplus_value = np.trapz(y=cons_surplus["price"], x=cons_surplus.index)
square_cons = (cons_surplus.index.max() - cons_surplus.index.min()) * eq_price
cons_surplus_value -= square_cons
cons_surplus_value

# %% [markdown]
# ## Producer surplus

# %%
# The same but with producer surplus.
prod_surplus = supply_demand.copy()
# Isolate the producer surplus part.
prod_surplus = prod_surplus[prod_surplus.index < eq_price]["supply"]
prod_surplus = prod_surplus.reset_index().set_index("supply")
prod_surplus = prod_surplus.sort_index()
prod_surplus = prod_surplus[prod_surplus.index.notnull()]
# prod_surplus.plot()

# %% [markdown]
# ![image.png](attachment:image.png)

# %%
# The area above the line is producer surplus.
# In this case we first need to find the square of the rectangle
# and subtract the square below the line.
ps_i = prod_surplus.index
ps_pr = prod_surplus["price"]
square_prod = (ps_i.max() - ps_i.min()) * ps_pr.max()
down_square_prod = np.trapz(y=ps_pr, x=ps_i)
prod_surplus_value = square_prod - down_square_prod
prod_surplus_value

# %% [markdown]
# ## Total surplus

# %%
total_surplus = prod_surplus_value + cons_surplus_value
total_surplus


# %% [markdown]
# # PDF of economic surplus

# %%
# Create messy function for calculating economic surplus.
def calculate_economic_surplus(df, eq_price):
    # Consumer surplus.
    cons_surplus = df.copy()
    cons_surplus = cons_surplus[cons_surplus.index > eq_price]["demand"]
    cons_surplus = cons_surplus.reset_index().set_index("demand")
    cons_surplus = cons_surplus.sort_index()
    cons_surplus = cons_surplus[cons_surplus.index.notnull()]
    cons_surplus_value = np.trapz(y=cons_surplus["price"], x=cons_surplus.index)
    square_cons = (cons_surplus.index.max() - cons_surplus.index.min()) * eq_price
    cons_surplus_value -= square_cons
    # Producer surplus.
    prod_surplus = df.copy()
    prod_surplus = prod_surplus[prod_surplus.index < eq_price]["supply"]
    prod_surplus = prod_surplus.reset_index().set_index("supply")
    prod_surplus = prod_surplus.sort_index()
    prod_surplus = prod_surplus[prod_surplus.index.notnull()]
    ps_i = prod_surplus.index
    ps_pr = prod_surplus["price"]
    square_prod = (ps_i.max() - ps_i.min()) * ps_pr.max()
    down_square_prod = np.trapz(y=ps_pr, x=ps_i)
    prod_surplus_value = square_prod - down_square_prod
    #
    total_surplus = prod_surplus_value + cons_surplus_value
    return total_surplus


# %%
# Check if the fucntion is true.
calculate_economic_surplus(supply_demand, eq_price) == total_surplus

# %%
# MC simulation for obtaining economic surplus.
ec_surplus_df = pd.DataFrame()
for i in range(1, 500):
    ba_raw = ralololi.get_data(n_samples)
    sd = ralololi.get_supply_demand_curve(ba_raw)
    eq_p, _ = ralololi.find_equilibrium(sd, print_graph=False)
    total_surplus = calculate_economic_surplus(sd, eq_p)
    ec_surplus_df.loc[i, "econ_surplus"] = total_surplus

# %%
ec_surplus_df.hist(bins=20)

# %% [markdown]
# ## Distribution of economic surplus depending on N

# %% run_control={"marked": false}
# MC simulation for obtaining economic surplus with random N.
ec_surplus_df_N = pd.DataFrame()
for i in range(1, 1000):
    N = random.choice(range(5, 1000))
    ba_raw = ralololi.get_data(N)
    sd = ralololi.get_supply_demand_curve(ba_raw)
    eq_p, _ = ralololi.find_equilibrium(sd, print_graph=False)
    total_surplus = calculate_economic_surplus(sd, eq_p)
    ec_surplus_df_N.loc[i, "N"] = N
    ec_surplus_df_N.loc[i, "econ_surplus"] = total_surplus

# %%
ec_surplus_df_N["econ_surplus"].hist(bins=20)

# %% [markdown]
# More of a uniform distribution.

# %% run_control={"marked": false}
ec_surplus_df_N.set_index("N").sort_index().plot()


# %% [markdown]
# Strong dependence of economic surplus with respect to N.

# %% [markdown]
# # Compare economic surplus of "big" and "small" markets

# %%
def generate_three_numbers_with_given_sum(
    final_sum: int, threshold: int = 10
) -> list:
    """
    Randomly generate three numbers which sum will equal to the given number.
    """
    numbers = sorted(random.sample(range(final_sum), 2))
    num1 = numbers[0]
    num2 = numbers[1] - numbers[0]
    num3 = final_sum - numbers[1]
    final_list = [num1, num2, num3]
    for x in final_list:
        if x < threshold:
            final_list = generate_three_numbers_with_given_sum(final_sum)
    return final_list


def simulate_economic_surplus(N: int):
    ba_raw = ralololi.get_data(N)
    sd = ralololi.get_supply_demand_curve(ba_raw)
    eq_p, eq_q = ralololi.find_equilibrium(sd, print_graph=False)
    total_surplus = calculate_economic_surplus(sd, eq_p)
    return total_surplus, eq_p, eq_q


# %%
# MC simulation for comparing economic surplus between "big" and "small" markets.
ec_surplus_df_N_markets = pd.DataFrame()
for i in range(1, 1000):
    # Generate the number of participants in a "big" market.
    N = random.choice(range(100, 500))
    # Simulate economic surplus for a "big" market.
    total_surplus_big, eq_p_big, eq_q_big = simulate_economic_surplus(N)
    ec_surplus_df_N_markets.loc[i, "N_big"] = N
    ec_surplus_df_N_markets.loc[i, "eq_p_big"] = eq_p_big
    ec_surplus_df_N_markets.loc[i, "eq_q_big"] = eq_q_big
    ec_surplus_df_N_markets.loc[i, "econ_surplus_big"] = total_surplus_big

    # Generate the number of participants in a three "small" markets.
    N_small = generate_three_numbers_with_given_sum(N)
    n1 = N_small[0]
    n2 = N_small[1]
    n3 = N_small[2]
    #
    total_surplus1, eq_p1, eq_q1 = simulate_economic_surplus(n1)
    ec_surplus_df_N_markets.loc[i, "n1"] = n1
    ec_surplus_df_N_markets.loc[i, "eq_p1"] = eq_p1
    ec_surplus_df_N_markets.loc[i, "eq_q1"] = eq_q1
    ec_surplus_df_N_markets.loc[i, "econ_surplus1"] = total_surplus1
    #
    total_surplus2, eq_p2, eq_q2 = simulate_economic_surplus(n2)
    ec_surplus_df_N_markets.loc[i, "n2"] = n2
    ec_surplus_df_N_markets.loc[i, "eq_p2"] = eq_p2
    ec_surplus_df_N_markets.loc[i, "eq_q2"] = eq_q2
    ec_surplus_df_N_markets.loc[i, "econ_surplus2"] = total_surplus2
    #
    total_surplus3, eq_p3, eq_q3 = simulate_economic_surplus(n3)
    ec_surplus_df_N_markets.loc[i, "n3"] = n3
    ec_surplus_df_N_markets.loc[i, "eq_p3"] = eq_p3
    ec_surplus_df_N_markets.loc[i, "eq_q3"] = eq_q3
    ec_surplus_df_N_markets.loc[i, "econ_surplus3"] = total_surplus3
    #
    ec_surplus_df_N_markets.loc[i, "econ_surplus_small_total"] = (
        total_surplus1 + total_surplus2 + total_surplus3
    )

# %%
big_small_surpluses = (
    ec_surplus_df_N_markets[
        ["N_big", "econ_surplus_big", "econ_surplus_small_total"]
    ]
    .set_index("N_big")
    .sort_index()
)
big_small_surpluses.plot()

# %%
big_small_diff = (
    big_small_surpluses["econ_surplus_big"]
    - big_small_surpluses["econ_surplus_small_total"]
)
print(f"Mean value of difference: {big_small_diff.mean()}")
big_small_diff.plot()
