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

import core.plotting.plotting_utils as cplpluti
import helpers.hdbg as hdbg
import helpers.hprint as hprint

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

hprint.config_notebook()

# %% [markdown]
# # Exchanges (Spot)

# %%
# Read .html file.
file_name_exch = (
    "Top Cryptocurrency Exchanges Ranked By Volume _ CoinMarketCap.html"
)
file_exch = pd.read_html(file_name_exch)
# Select necessary columns with top-100.
exch_df = file_exch[0][["Name", "Volume(24h)", "Exchange Score", "#"]].loc[:99]


# %%
def volume_transformer(vol_str):
    vol_str_split = vol_str.split(",")
    last_piece = vol_str_split[-1]
    last_piece = last_piece[0:3]
    # print(poo_split)
    result = [p for p in vol_str_split[:-1]]
    result.append(last_piece)
    return ",".join(result)


# %%
# Clean up.
exch_df["Name"] = exch_df["Name"].str.replace("\d+", "")

exch_df["Volume(24h)"] = exch_df["Volume(24h)"].apply(
    lambda x: volume_transformer(x)
)
exch_df["Volume(24h)"] = exch_df["Volume(24h)"].apply(
    lambda x: x.replace("$", "")
)
exch_df["Volume(24h)"] = exch_df["Volume(24h)"].apply(
    lambda x: x.replace(".", "")
)
exch_df["Volume(24h)"] = exch_df["Volume(24h)"].apply(
    lambda x: x.replace(",", "")
)
exch_df["Volume(24h)"] = pd.to_numeric(exch_df["Volume(24h)"])

# %%
# Select only `good` exchanges.
top_exch = exch_df[exch_df["Exchange Score"] > 6]
top_exch

# %%
# Sort by volume.
top_exch = top_exch.sort_values(
    ["Volume(24h)"], ascending=False, ignore_index=True
)
# Plot top-10 exhanges by volume.
display(top_exch.loc[:9])
cplpluti.plot_barplot(top_exch.set_index("Name")["Volume(24h)"].iloc[:10])

# %%
# Calculate the total trading volume of `good` exchanges.
top_exch_volume = top_exch["Volume(24h)"].sum()
# Calculate the total trading volume of top-10 exchanges.
top10_exch_volume = top_exch.loc[:9]["Volume(24h)"].sum()
# The first 10 exchanges trade 90% of volume on `good` exchanges.
top10_exch_volume / top_exch_volume

# %%
# Plot the ratio.
ratio_good = pd.DataFrame()
ratio_good.loc["Top-10 exchanges", "Trading volume"] = top10_exch_volume
ratio_good.loc["Other `good` exchanges", "Trading volume"] = (
    top_exch_volume - top10_exch_volume
)
cplpluti.plot_barplot(ratio_good["Trading volume"])

# %%
# Calculate the total trading volume of all exchanges (100 items).
exch_volume = exch_df["Volume(24h)"].sum()
# Calculate the total trading volume of all `good` exchanges.
good_exch_volume = top_exch["Volume(24h)"].sum()
# `Good` exchanges trade 40% of volume.
good_exch_volume / exch_volume

# %%
# Plot the ratio.
ratio_all = pd.DataFrame()
ratio_all.loc["`Good` exchanges", "Trading volume"] = good_exch_volume
ratio_all.loc["Other exchanges", "Trading volume"] = (
    exch_volume - good_exch_volume
)
cplpluti.plot_barplot(ratio_all["Trading volume"])

# %% [markdown]
# Here, the results are a little bit suprising. Maybe inferior exchanges provide incorrect statistics (and that's why their score is low).

# %% [markdown]
# # Exchanges (Derivatives)

# %%
# Read .html file.
file_name_der = (
    "Top Cryptocurrency Derivatives Exchanges Ranked _ CoinMarketCap.html"
)
file_der = pd.read_html(file_name_der)
# Select necessary columns with top-100.
exch_der_df = file_der[0][["#", "Name", "Volume(24h)"]]

# %%
# Clean up.
exch_der_df["Name"] = exch_der_df["Name"].str.replace("\d+", "")

exch_der_df["Volume(24h)"] = exch_der_df["Volume(24h)"].apply(
    lambda x: volume_transformer(x)
)
exch_der_df["Volume(24h)"] = exch_der_df["Volume(24h)"].apply(
    lambda x: x.replace(",", "")
)
exch_der_df["Volume(24h)"] = exch_der_df["Volume(24h)"].apply(
    lambda x: x.replace("$", "")
)
exch_der_df["Volume(24h)"] = pd.to_numeric(exch_der_df["Volume(24h)"])
# Sorting.
exch_der_df = exch_der_df.sort_values("Volume(24h)", ascending=False)

# %%
# Plot top-10 exchanges by volume.
display(exch_der_df.loc[:9])
cplpluti.plot_barplot(exch_der_df.set_index("Name")["Volume(24h)"].iloc[:10])

# %%
# Calculate the total trading volume of derivatives exchanges.
top_exch_der_volume = exch_der_df["Volume(24h)"].sum()
# Calculate the total trading volume of top-10 derivatives exchanges.
top10_exch_der_volume = exch_der_df.loc[:9]["Volume(24h)"].sum()
# The first 10 exchanges trade 79% of volume on `good` exchanges.
top10_exch_der_volume / top_exch_der_volume

# %%
# Plot the ratio.
ratio_der = pd.DataFrame()
ratio_der.loc["Top-10 exchanges", "Trading volume"] = top10_exch_der_volume
ratio_der.loc["Other exchanges", "Trading volume"] = (
    top_exch_der_volume - top10_exch_der_volume
)
cplpluti.plot_barplot(ratio_der["Trading volume"])

# %% [markdown]
# # Cryptocurrencies

# %%
# Load and process the data.
url = "https://api.coinmarketcap.com/data-api/v3/cryptocurrency/listing?start=1&limit=10081&sortBy=market_cap&sortType=desc&convert=USD&cryptoType=all&tagType=all&audited=false&aux=name,volume_24h"
response = requests.get(url)

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
# Leave only necessary columns.
crypto_df = crypto_df[["name", "_volume24h", "_marketCap"]]

# %%
# Cryptocurrencies by Market Cap.
display(crypto_df.loc[:9])
cplpluti.plot_barplot(crypto_df.set_index("name")["_marketCap"].iloc[:10])

# %%
# Calculate the total Market Cap of top-10 cc.
top10_cc_mc = crypto_df.iloc[:10]["_marketCap"].sum()
# Calculate the total Market Cap of top-100 cc.
top100_cc_mc = crypto_df.iloc[:100]["_marketCap"].sum()
# The first 10 cc trade 83% of Market Cap of top-100 cc.
top10_cc_mc / top100_cc_mc

# %%
# Plot the ratio.
ratio_cc = pd.DataFrame()
ratio_cc.loc["Top-10 cryptocurrencies", "Market Cap"] = top10_cc_mc
ratio_cc.loc["Other cryptocurrencies from top-100", "Market Cap"] = (
    top100_cc_mc - top10_cc_mc
)
cplpluti.plot_barplot(ratio_cc["Market Cap"])

# %%
# Cryptocurrencies by last 24h volume.
display(crypto_df.sort_values("_volume24h", ascending=False).iloc[:10])
cplpluti.plot_barplot(
    crypto_df.set_index("name")
    .sort_values("_volume24h", ascending=False)["_volume24h"]
    .iloc[:10]
)

# %%
# Calculate the total trading volume of top-10 cc.
top10_cc_volume = (
    crypto_df.sort_values("_volume24h", ascending=False)
    .iloc[:10]["_volume24h"]
    .sum()
)
# Calculate the total trading volume of top-100 cc.
top100_cc_volume = (
    crypto_df.sort_values("_volume24h", ascending=False)
    .iloc[:100]["_volume24h"]
    .sum()
)
# The first 10 cc trade 81% of volume of top-100 cc.
top10_cc_volume / top100_cc_volume

# %%
# Plot the ratio.
ratio_volume = pd.DataFrame()
ratio_volume.loc["Top-10 cryptocurrencies", "Trading volume"] = top10_cc_volume
ratio_volume.loc["Other cryptocurrencies from top-100", "Trading volume"] = (
    top100_cc_volume - top10_cc_volume
)
cplpluti.plot_barplot(ratio_volume["Trading volume"])
