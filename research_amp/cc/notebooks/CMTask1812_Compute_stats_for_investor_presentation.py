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
# # Functiuons

# %%
def volume_transformer(vol_str):
    vol_str_split = vol_str.split(",")
    last_piece = vol_str_split[-1]
    last_piece = last_piece[0:3]
    result = [p for p in vol_str_split[:-1]]
    result.append(last_piece)
    return ",".join(result)

def clean_up_exchanges(df):
    # Names.
    df["Name"] = df["Name"].str.replace("\d+", "")
    # Volumes.
    df["Volume(24h)"] = df["Volume(24h)"].apply(
        lambda x: volume_transformer(x)
    )
    df["Volume(24h)"] = df["Volume(24h)"].apply(
        lambda x: x.replace("$", "")
    )
    df["Volume(24h)"] = df["Volume(24h)"].apply(
        lambda x: x.replace(".", "")
    )
    df["Volume(24h)"] = df["Volume(24h)"].apply(
        lambda x: x.replace(",", "")
    )
    df["Volume(24h)"] = pd.to_numeric(df["Volume(24h)"])
    return df

def get_cumulative_volume_ratios(df, value_col, plot_results):
    cumul_volume = df[value_col].cumsum() / df[value_col].sum()
    if plot_results:
        cumul_volume.plot()
    return cumul_volume

def get_general_volume_ratio_df(df1, df2, value_col, col1, col2, plot_results):
    value1 = df1[value_col].sum()
    value2 = df2[value_col].sum()
    ratio_df = pd.DataFrame()
    ratio_df.loc[col1, "Trading volume"] = value2
    ratio_df.loc[col2, "Trading volume"] = (
            value1 - value2
    )
    if plot_results:
        cplpluti.plot_barplot(ratio_df["Trading volume"])
    return ratio_df


# %% [markdown]
# # Exchanges (Spot)

# %%
# Read .html file.
# It was downloaded 2022-05-03 from https://coinmarketcap.com/rankings/exchanges/.
# Contains the snapshot of a table with the descriptive statistics of cryptocurrency exhanges.
file_name_exch = (
    "Top Cryptocurrency Exchanges Ranked By Volume _ CoinMarketCap.html"
)
file_exch = pd.read_html(file_name_exch)
# Select necessary columns with top-100.
exch_df = file_exch[0][["Name", "Volume(24h)", "Exchange Score", "#"]].loc[:99]

# %%
# Clean up the data.
all_exch = clean_up_exchanges(exch_df)

# %%
# Select only `good` exchanges.
# Note: `good` exchanges - the ones with the Score > 6.
# `All` exchanges - `good` exchanges + the remaining ones.
good_exch = all_exch[all_exch["Exchange Score"] > 6]
good_exch

# %%
# Sort by volume.
good_exch = good_exch.sort_values(
    ["Volume(24h)"], ascending=False, ignore_index=True
)
# Plot top-10 exhanges by volume.
display(good_exch.loc[:9])
cplpluti.plot_barplot(good_exch.set_index("Name")["Volume(24h)"].iloc[:10])

# %%
ratio_good = get_general_volume_ratio_df(good_exch, 
                                         good_exch.loc[:9], 
                                         "Volume(24h)",
                                         "Top-10 `good` exchanges", 
                                         "Other `good` exchanges", 
                                         plot_results=True)
display(ratio_good)

# %%
# Plot the cumulative volume against exchanges.
good_cumul_volume = get_cumulative_volume_ratios(good_exch, "Volume(24h)", plot_results=True)
display(good_cumul_volume.head(5))

# %%
ratio_all = get_general_volume_ratio_df(all_exch, 
                                         good_exch, 
                                        "Volume(24h)",
                                         "`Good` exchanges", 
                                         "Other exchanges", 
                                         plot_results=True)
display(ratio_all)

# %% [markdown]
# Here, the results are a little bit suprising. Maybe inferior exchanges provide incorrect statistics (and that's why their score is low).

# %%
# Sort all exchanges by volume.
all_exch_sorted = all_exch.sort_values(
    ["Volume(24h)"], ascending=False, ignore_index=True
)
# Plot the cumulative volume against exchanges.
all_cumul_volume = get_cumulative_volume_ratios(all_exch_sorted, "Volume(24h)", plot_results=True)
display(all_cumul_volume.head(5))

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
exch_der_df = clean_up_exchanges(exch_der_df)
# Sorting.
exch_der_df = exch_der_df.sort_values("Volume(24h)", ascending=False)

# %%
# Plot top-10 exchanges by volume.
display(exch_der_df.loc[:9])
cplpluti.plot_barplot(exch_der_df.set_index("Name")["Volume(24h)"].iloc[:10])

# %%
ratio_der = get_general_volume_ratio_df(exch_der_df, 
                                         exch_der_df.loc[:9], 
                                        "Volume(24h)",
                                         "Top-10 exchanges", 
                                         "Other exchanges", 
                                         plot_results=True)
display(ratio_der)

# %% run_control={"marked": false}
# Plot the cumulative volume against exchanges.
all_cumul_der_volume = get_cumulative_volume_ratios(exch_der_df, "Volume(24h)", plot_results=True)
display(all_cumul_der_volume.head(5))

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
ratio_cc = get_general_volume_ratio_df(crypto_df.iloc[:100], 
                                       crypto_df.iloc[:10],
                                        "_marketCap",
                                         "Top-10 cryptocurrencies", 
                                         "Other cryptocurrencies from top-100", 
                                         plot_results=True)
display(ratio_cc)

# %%
cc_mc_sorted = crypto_df.sort_values("_marketCap", ascending=False, ignore_index=True)
# Plot the cumulative volume against exchanges.
all_cumul_cc_mc = get_cumulative_volume_ratios(cc_mc_sorted, "_marketCap", plot_results=True)
display(all_cumul_cc_mc.head(5))

# %%
# Cryptocurrencies by last 24h volume.
display(crypto_df.sort_values("_volume24h", ascending=False).iloc[:10])
cplpluti.plot_barplot(
    crypto_df.set_index("name")
    .sort_values("_volume24h", ascending=False)["_volume24h"]
    .iloc[:10]
)

# %%
ratio_cc_volume = get_general_volume_ratio_df(crypto_df.sort_values("_volume24h", ascending=False).iloc[:100],
                                              crypto_df.sort_values("_volume24h", ascending=False).iloc[:10],
                                        "_volume24h",
                                         "Top-10 cryptocurrencies", 
                                         "Other cryptocurrencies from top-100", 
                                         plot_results=True)
display(ratio_cc_volume)

# %%
cc_volume_sorted = crypto_df.sort_values("_volume24h", ascending=False, ignore_index=True)
# Plot the cumulative volume against exchanges.
all_cumul_cc_volume = get_cumulative_volume_ratios(cc_volume_sorted, "_volume24h", plot_results=True)
display(all_cumul_der_volume.head(5))
