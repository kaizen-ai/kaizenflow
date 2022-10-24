import numpy as np
import pandas as pd
import scipy as scipy

import helpers.hdbg as hdbg

np.random.seed(seed=1806)

def get_data():
    n_samples = 5
    ask_mean = 1.0
    ask_std = 2.0
    bid_mean = -1.0
    bid_std = ask_std

    ask_rv = scipy.stats.norm.rvs(loc=ask_mean, scale=ask_std, size=n_samples)
    bid_rv = scipy.stats.norm.rvs(loc=bid_mean, scale=bid_std, size=n_samples)
    ob = pd.DataFrame([bid_rv, ask_rv]).T
    ob.columns = ["bids", "asks"]
    return ob


def _get_supply_demand_curve(srs: pd.Series, mode: str) -> pd.Series:
    ascending = mode == "supply"
    # For each value compute the cumsum.
    index = srs.sort_values(ascending=ascending)
    values = [1] * srs.shape[0]
    vals = pd.Series(values, index=index)
    ret = vals.cumsum()
    ret.name = mode
    return ret


def get_supply_demand_curve(ob):
    supply = _get_supply_demand_curve(ob["bids"], "supply")
    demand = _get_supply_demand_curve(ob["asks"], "demand")
    df = supply.to_frame().merge(demand, how="outer", left_index=True, right_index=True)
    df = df.interpolate()
    return df


def find_equilibrium(sd):
    supply = sd["supply"]
    demand = sd["demand"]
    excess = supply - demand
    idx = np.where(np.diff(np.sign(excess)) == 2)[0]
    hdbg.dassert_eq(idx.shape[0], 1)
    idx = idx[0]
    hdbg.dassert_lte(excess.iloc[idx], 0)
    hdbg.dassert_lte(0, excess.iloc[idx + 1])
    return idx
