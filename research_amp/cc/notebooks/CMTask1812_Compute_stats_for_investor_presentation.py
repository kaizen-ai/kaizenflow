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
# # Imports

# %%
import json
import logging

import pandas as pd
import requests
import seaborn as sns

import helpers.hdbg as hdbg
import helpers.hprint as hprint

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

hprint.config_notebook()

# %% [markdown]
# # Exchanges

# %% [markdown]
# ## Load data

# %%
url = "https://coinmarketcap.com/rankings/exchanges/"
response = requests.get(url)
df = pd.read_html(response.text)[0]
df.loc[df["Volume(24h)"].isna()].shape

# %%
# Almost all the loaded data from url have NaNs so loading from fully saved html.
with open("top.html", "r") as f:
    df = pd.read_html(f.read())[0]
_LOG.info(df.shape)
df.head(3)

# %%
# Check for NaNs.
df.loc[df["Volume(24h)"].isna()].shape

# %%
# Let's set it to an empty string first.
df.loc[df["Volume(24h)"].isna()] = ""
df.loc[df["Volume(24h)"].isna()].shape

# %% [markdown]
# ## Create data frame with columns `name`, `volume`.

# %%
name_volume_df = df[["Name", "Volume(24h)"]]
name_volume_df.head(3)

# %% [markdown]
# ### Convert types

# %%
# Clear and convert volume to integer.

name_volume_df = name_volume_df.convert_dtypes()
name_volume_df["Volume(24h)"] = name_volume_df["Volume(24h)"].map(
    lambda x: x[1:-6]
)
name_volume_df.loc[name_volume_df["Volume(24h)"] == ""] = "0"
name_volume_df["Volume(24h)"] = name_volume_df["Volume(24h)"].apply(
    lambda x: x.replace(",", "")
)
name_volume_df["Volume(24h)"] = pd.to_numeric(name_volume_df["Volume(24h)"])
name_volume_df.head(3)

# %% [markdown]
# ### Sorting by `volume`

# %%
name_volume_df.sort_values(
    ["Volume(24h)"], ascending=False, ignore_index=True, inplace=True
)
name_volume_df.head(3)

# %% [markdown]
# ### Cumulative sum

# %%
cumsum = pd.DataFrame(name_volume_df["Volume(24h)"].cumsum())
cumsum.head()

# %%
# Cumulative sum of top-10 exchanges.
sns.barplot(x=name_volume_df["Name"][:10], y=cumsum["Volume(24h)"][:10])

# %% [markdown]
# # Crypto currencies

# %% [markdown]
# ## Load data

# %%
url = "https://api.coinmarketcap.com/data-api/v3/cryptocurrency/listing?start=1&limit=10081&sortBy=market_cap&sortType=desc&convert=USD&cryptoType=all&tagType=all&audited=false&aux=name,volume_24h"
response = requests.get(url)

# %% [markdown]
# ## Convert json to data frame

# %%
crypto_data = json.loads(response.text)
crypto_df = pd.json_normalize(
    crypto_data["data"]["cryptoCurrencyList"],
    "quotes",
    ["name"],
    record_prefix="_",
)
_LOG.info(crypto_df.shape)
crypto_df.head(3)

# %%
name_volume_crypto_df = crypto_df[["name", "_volume24h"]]
name_volume_crypto_df.head(3)

# %% [markdown]
# ## Sorting by `volume`

# %%
name_volume_crypto_df.sort_values(
    ["_volume24h"], ascending=False, ignore_index=True, inplace=True
)
name_volume_crypto_df.head(3)

# %% [markdown]
# ##  Cumulative sum

# %%
cumsum = pd.DataFrame(name_volume_crypto_df["_volume24h"].cumsum())
cumsum.head()

# %%
# Cumulative sum of top-10 crypto currencies.
sns.barplot(x=name_volume_crypto_df["name"][:10], y=cumsum["_volume24h"][:10])
