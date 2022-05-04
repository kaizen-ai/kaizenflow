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
print(f"Hit Rate: {round(rets_df.prediction_is_right.sum()/rets_df.shape[0], 4)*100}%")
