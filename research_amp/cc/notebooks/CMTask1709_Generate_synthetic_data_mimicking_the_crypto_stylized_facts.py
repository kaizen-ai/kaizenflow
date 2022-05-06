# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.13.8
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# Compute random returns as Gaussians or t-distributions

# %% [markdown]
# # Imports

# %%
import core.finance.market_data_example as cfmadaex
from typing import List
import im_v2.common.universe as ivcu
import requests
import helpers.hdatetime as hdateti
import statsmodels

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import core.finance as cofinanc
import dataflow.core as dtfcore
import dataflow.system.source_nodes as dtfsysonod
import core.artificial_signal_generators as carsigen

# %% [markdown]
# # Generate the data

# %% [markdown]
# ## Using `generate_random_price_data` for multiple assets

# %% [markdown]
# Problems:
# - Generating prices are the same for all assets
# - Distribution of returns is not normal

# %%
start_datetime = pd.Timestamp("2021-01-01")
end_datetime = pd.Timestamp("2021-01-31")
columns = ["close"]
asset_ids = list(range(2))
freq = "1T"
initial_price = 29000
seed = 100,

df = cfmadaex.generate_random_price_data(
start_datetime,
end_datetime,
columns,
asset_ids,
freq=freq,
initial_price=initial_price,
seed=seed)

df = df.set_index("timestamp_db").drop(columns=["start_datetime", "end_datetime"])
df = dtfsysonod._convert_to_multiindex(df, "asset_id")
df.head()

# %%
node_returns_config = {
    "in_col_groups": [
        ("close",),
    ],
    "out_col_group": (),
    "transformer_kwargs": {
        "mode": "pct_change",
    },
    "col_mapping": {
        "close": "close.ret_0",
    },
}
# Create the node that computes ret_0.
nid = "ret0"
node = dtfcore.GroupedColDfToDfTransformer(
    nid,
    transformer_func=cofinanc.compute_ret_0,
    **node_returns_config,
)
# Compute the node on the data.
rets = node.fit(df)

# %%
df = rets["df_out"]
df.head(5)

# %%
df.swaplevel(axis=1)[0][["close"]].plot(figsize=(15,7))

# %% [markdown]
# ## Using `generate_random_price_data` for one asset

# %% [markdown]
# Problems:
# - Distribution of returns is not normal

# %%
start_datetime = pd.Timestamp("2021-01-01")
end_datetime = pd.Timestamp("2021-01-31")
columns = ["close"]
asset_ids = [1]
freq = "1T"
initial_price = 29000
seed = 100,

df = cfmadaex.generate_random_price_data(
start_datetime,
end_datetime,
columns,
asset_ids,
freq=freq,
initial_price=initial_price,
seed=seed)

df = df.set_index("timestamp_db").drop(columns=["start_datetime", "end_datetime", "asset_id"])
df["rets"] = cofinanc.compute_ret_0(df[["close"]], "pct_change")
df.head(3)

# %%
df[["close"]].plot(figsize=(15,7))
df[["rets"]].plot(figsize=(15,7))

# %%
fig = plt.figure(figsize=(15, 7))
ax1 = fig.add_subplot(1, 1, 1)
df['rets'].hist(bins=50, ax=ax1)
ax1.set_xlabel('Return')
ax1.set_ylabel('Sample')
ax1.set_title('Return distribution')
plt.show()

# %% [markdown]
# ## Using `generate_random_bars`

# %% [markdown]
# Problems:
# - Strange outcomes

# %% run_control={"marked": false}
start_datetime = pd.Timestamp("2021-01-01")
end_datetime = pd.Timestamp("2021-01-31")

df = cfmadaex.generate_random_bars(
    start_datetime,
    end_datetime,
    asset_ids = [1,2],
)

df = df.set_index("timestamp_db").drop(columns=["start_datetime", "end_datetime"])
df = dtfsysonod._convert_to_multiindex(df, "asset_id")
df.head()

# %%
df["close"].plot(figsize=(15,7))

# %%
node_returns_config = {
    "in_col_groups": [
        ("close",),
    ],
    "out_col_group": (),
    "transformer_kwargs": {
        "mode": "pct_change",
    },
    "col_mapping": {
        "close": "close.ret_0",
    },
}
# Create the node that computes ret_0.
nid = "ret0"
node = dtfcore.GroupedColDfToDfTransformer(
    nid,
    transformer_func=cofinanc.compute_ret_0,
    **node_returns_config,
)
# Compute the node on the data.
rets = node.fit(df)
rets["df_out"].head(3)

# %%
rets["df_out"]["close.ret_0"].plot(figsize=(15,7))

# %% [markdown]
# ## Using returns directly (`get_gaussian_walk`)

# %% [markdown]
# Seems correct

# %%
drift = 0
vol = 0.2
size = 252
seed = 10
rets = carsigen.get_gaussian_walk(drift, vol, size, seed=seed).diff()

# %%
rets.plot()

# %%
fig = plt.figure(figsize=(15, 7))
ax1 = fig.add_subplot(1, 1, 1)
rets.hist(bins=100, ax=ax1)
ax1.set_xlabel('Return')
ax1.set_ylabel('Sample')
ax1.set_title('Returns distribution')
plt.show()

# %% [markdown]
# For each bar, generate random predictions with a given hit rate
# - Compute the hit rate to confirm
# - def hit_rate(y, y_hat):
#

# %%
rets_df = rets.to_frame().iloc[1:]
rets_df.columns = ["rets"]

# 0 -> price goes down
# 1 -> price goes up
rets_df["prediction"] = np.random.randint(0, 2, rets_df.shape[0])
# Get the difference to estimate the validity of prediciton.
rets_df["diff"] = rets_df.rets.diff()
# Estimate the prediciton.
rets_df["is_prediction_right"] = ((rets_df["diff"]>0)&(rets_df["prediction"]==1))|((rets_df["diff"]<0)&(rets_df["prediction"]==0))
# Get rid of unnecessary columns
rets_df = rets_df.drop(columns=["diff"])
rets_df = rets_df.iloc[1:]

# %%
rets_df

# %%
print(f"Hit Rate: {round(rets_df.is_prediction_right.sum()/rets_df.shape[0], 4)*100}%")


# %% [markdown]
# # Extract returns from the real data

# %% [markdown]
# ## Load BTC data from CC

# %%
def get_exchange_currency_for_api_request(full_symbol: str) -> str:
    """
    Returns `exchange_id` and `currency_pair` in a format for requests to cc
    API.
    """
    cc_exchange_id, cc_currency_pair = ivcu.parse_full_symbol(full_symbol)
    cc_currency_pair = cc_currency_pair.lower().replace("_", "-")
    return cc_exchange_id, cc_currency_pair


def load_crypto_chassis_ohlcv_for_one_symbol(full_symbol: str) -> pd.DataFrame:
    """
    - Transform CK `full_symbol` to the `crypto-chassis` request format.
    - Construct OHLCV data request for `crypto-chassis` API.
    - Save the data as a DataFrame.
    """
    # Deconstruct `full_symbol`.
    cc_exchange_id, cc_currency_pair = get_exchange_currency_for_api_request(
        full_symbol
    )
    # Build a request.
    r = requests.get(
        f"https://api.cryptochassis.com/v1/ohlc/{cc_exchange_id}/{cc_currency_pair}?startTime=0"
    )
    # Get url with data.
    url = r.json()["historical"]["urls"][0]["url"]
    # Read the data.
    df = pd.read_csv(url, compression="gzip")
    return df


def apply_ohlcv_transformation(
    df: pd.DataFrame,
    full_symbol: str,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> pd.DataFrame:
    """
    The following transformations are applied:

    - Convert `timestamps` to the usual format.
    - Convert data columns to `float`.
    - Add `full_symbol` column.
    """
    # Convert `timestamps` to the usual format.
    df = df.rename(columns={"time_seconds": "timestamp"})
    df["timestamp"] = df["timestamp"].apply(
        lambda x: hdateti.convert_unix_epoch_to_timestamp(x, unit="s")
    )
    df = df.set_index("timestamp")
    # Convert to `float`.
    for cols in df.columns:
        df[cols] = df[cols].astype(float)
    # Add `full_symbol`.
    df["full_symbol"] = full_symbol
    # Note: I failed to put [start_time, end_time] to historical request.
    # Now it loads all the available data.
    # For that reason the time interval is hardcoded on this stage.
    df = df.loc[(df.index >= start_date) & (df.index <= end_date)]
    return df


def read_crypto_chassis_ohlcv(
    full_symbols: List[str], start_date: pd.Timestamp, end_date: pd.Timestamp
) -> pd.DataFrame:
    """
    - Load the raw data for one symbol.
    - Convert it to CK format.
    - Repeat the first two steps for all `full_symbols`.
    - Concentrate them into unique DataFrame.
    """
    result = []
    for full_symbol in full_symbols:
        # Load raw data.
        df_raw = load_crypto_chassis_ohlcv_for_one_symbol(full_symbol)
        # Process it to CK format.
        df = apply_ohlcv_transformation(df_raw, full_symbol, start_date, end_date)
        result.append(df)
    final_df = pd.concat(result)
    return final_df


# %%
btc_df = read_crypto_chassis_ohlcv(["binance::BTC_USDT"], pd.Timestamp("2021-01-01", tz="UTC"), pd.Timestamp("2022-01-01", tz="UTC"))

# %%
# TODO(Max):

# Save data locally.
# Copy the data to /local/share/CMTask1709_...
# Copy from central location to the client
# Load data

# Once Grisha is done, you can just save / load directly there but for now you can just load / save locally.

# %% [markdown]
# ## Process returns

# %%
btc = btc_df.copy()

# %%
btc["rets"] = btc["close"].pct_change()

btc.describe()

# %% run_control={"marked": false}
# Calculate returns.
btc["rets"] = btc["close"].pct_change()
# Rolling SMA for returns (with 100 periods loockback period).
btc["rets_sma"] = btc["rets"].transform(lambda x: x.rolling(window=100).mean())
# Substract SMA from returns to remove the upward trend.
btc["rets_cleaned"] = btc["rets"] - btc["rets_sma"]
#btc["rets"].plot()
btc["rets_cleaned"].plot()

# %%
rets = btc[["rets"]]

# TODO(gp): Always use the dropna function that reports the number of nans removed
rets = rets[rets.rets.notna()]

# %% run_control={"marked": false}
fig = plt.figure(figsize=(15, 7))
ax1 = fig.add_subplot(1, 1, 1)
rets.hist(bins=300, ax=ax1)
ax1.set_xlabel('Return')
ax1.set_ylabel('Sample')
ax1.set_title('Returns distribution')
plt.show()

# %%
rets_df["rets"]

# %%
import helpers.hdbg as hdbg

# TODO(Max): Pass hit rate and 
def add_hit_rate(rets_df, hit_rate, seed):
    hdbg.dassert_isinstance(rets_df, pd.DataFrame)
    rets_df = rets_df.copy()
    #rets_df.columns = ["rets"]

    np.random.seed(seed)

    # (-1) -> price goes down
    # 1 -> price goes up
    uniform = np.random.randint(0, 2, rets_df.shape[0])
    #uniform = uniform - (2 * hit_rate - 1 - 0.5)
    rets_df["prediction"] = 2 * uniform - 1
    # Get the difference to estimate the validity of prediciton.
    #rets_df["diff"] = rets_df.rets.diff()
    # Estimate the prediciton.
    #rets_df["hit"] = ((rets_df["diff"]>0)&(rets_df["prediction"]==1))|((rets_df["diff"]<0)&(rets_df["prediction"]==-1))
    rets_df["hit"] = (rets_df["rets"] * rets_df["prediction"] >= 0)
    # Get rid of unnecessary columns
    #rets_df = rets_df.drop(columns=["diff"])
    #rets_df = rets_df.iloc[1:]

    #rets_df.head(3)
    return rets_df


# %%
dd = add_hit_rate(rets_df, 0.7, 1)
dd#print(rets_df["rets"])

# %%
dd["hit"].mean()

# %%
import random


# %% run_control={"marked": false}
# X ~ U[-1, 1], E[X] = 0
# hit_rate = 0

#n = df.shape[0]
def get_predictions(df, hit_rate, seed):
    n = df.shape[0]
    #hit_rate = 0.0

    rets = df["rets"].values
    #rets = rets[:n]
    #print("rets=", rets)

    # mask contains 1 for a desired hit and -1 for a miss.
    num_hits = int((1 - hit_rate) * n)
    mask = pd.Series(([-1] * num_hits) + ([1] * (n - num_hits)))

    
    random.shuffle(mask)
    #print("mask=", mask)
    #print(mask.mean())

    #print("sign(rets)=", np.sign(rets))
    #pred = pd.Series(np.sign(rets) * mask, index=df.index)
    pred = pd.Series(np.sign(rets) * mask)
    #print("pred=", pred)

    #hit_rate = (np.sign(pred * rets) >= 0).mean()
    #print(hit_rate)

    #print((pred * rets).mean())
    pred.index = df.index
    return pred

# %%
hit_rate = 0.8
dd = btc.head(1000)

# %%
dd["predictions"] = get_predictions(dd, hit_rate, 10)
dd = dd[["rets", "predictions"]]
dd["hit"] = (dd["rets"] * dd["predictions"] >= 0)
dd

# %%
dd["hit"].mean()


# %%

# %%

# %%
def calculate_confidence_interval(hit_series, alpha, method):
    """
    :param alpha: Significance level
    :param method: "normal", "agresti_coull", "beta", "wilson", "binom_test"
    """
    point_estimate = hit.mean()
    hit_lower, hit_upper = statsmodels.stats.proportion.proportion_confint(
        count=hit.sum(), nobs=hit.count(), alpha=alpha, method=method
    )
    result_values_pct = [100 * point_estimate, 100 * hit_lower, 100 * hit_upper]
    conf_alpha = (1 - alpha / 2) * 100
    print(f"hit_rate: {result_values_pct[0]}")
    print(f"hit_rate_lower_CI_({conf_alpha}%): {result_values_pct[1]}")
    print(f"hit_rate_upper_CI_({conf_alpha}%): {result_values_pct[2]}")


# %%
# Calculate confidence intervals.
hit = rets_df["hit"]
alpha = 0.05
method = "normal"
calculate_confidence_interval(hit, alpha, method)

# %%
# A model makes a prediction on the rets and then it's realized.
pnl = (rets_df["prediction"] * rets_df["rets"]).cumsum()

pnl.plot()


# %%
# Relationship between hit rate and pnl

# %%
def compute_pnl(rets_df):
    return (rets_df["prediction"] * rets_df["rets"]).sum()


# %%
compute_pnl(rets_df)

# %%
print(rets_df)

# %%
# Number of experiments per value of hit rate.
#n_experiment = 100
n_experiment = 1
# Every seed corresponds to a different "model".
seed = 10
results = {}
for hit_rate in np.linspace(0.4, 0.6, num=3):
    for i in range(n_experiment):
        df_tmp = add_hit_rate(rets_df, hit_rate, seed)
        hit_rate = df_tmp["hit"].mean()
        pnl = compute_pnl(df_tmp)
        results[hit_rate].append(pnl)
        print(hit_rate, pnl)
        seed += 1
        
        
# hit_rate -> pnls
# 0.4 -> [0.1, 0.2, 0.3]
# 0.5 -> []


# %%
# Step 1: split the notebooks in gallery and not
# - gallery notebook with the first 2-3 sessions showing the functions
# - 1709 contains only the good stuff
# Step 2: 
# - clean up get_predictions() and add 1 unit test that checks the schema of df, freeze the output
# Step 3:
# - bootstrapping (montecarlo simulation) to compute pnl = f(hit_rate)
