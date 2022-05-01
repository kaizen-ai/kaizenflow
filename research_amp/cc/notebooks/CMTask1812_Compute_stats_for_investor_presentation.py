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
import logging

import pandas as pd
import requests
import seaborn as sns

import helpers.hdbg as hdbg
import helpers.hprint as hprint

import core.plotting.plotting_utils as cplpluti

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

hprint.config_notebook()

# %% [markdown]
# # Load data

# %%
url = "https://coinmarketcap.com/rankings/exchanges/"
response = requests.get(url)
df = pd.read_html(response.text)[0]
df.loc[df["Volume(24h)"].isna()].shape

# %%
df.head(10)

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
# # Create data frame with columns `name`, `volume`.

# %%
columns = list(df.columns)
columns.pop(1)
columns.pop(2)
name_volume_df = df.copy().drop(columns=columns)
name_volume_df.head(3)

# %% [markdown]
# ## Convert types

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
# ## Sorting by `volume`

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
# # Max's version

# %% [markdown]
# ## Exchanges

# %% [markdown]
# Since the .html approach requires to load html file by yourseld, I will load data by myself and reuse some previous code.

# %%
# Read .html file.
file_name_exch = "Top Cryptocurrency Exchanges Ranked By Volume _ CoinMarketCap.html"
file_exch = pd.read_html(file_name_exch)
# Select necessary columns with top-100.
exch_df = file_exch[0][["Name", "Volume(24h)", "Exchange Score", "#"]].loc[:99]

# %%
# Clean up.
exch_df['Name'] = exch_df['Name'].str.replace('\d+', '')

exch_df["Volume(24h)"] = exch_df["Volume(24h)"].map(
    lambda x: x[1:-6]
)
exch_df["Volume(24h)"] = exch_df["Volume(24h)"].apply(
    lambda x: x.replace(",", "")
)
exch_df["Volume(24h)"] = pd.to_numeric(exch_df["Volume(24h)"])
exch_df.sort_values(
    ["Volume(24h)"], ascending=False, ignore_index=True, inplace=True
)

# %%
# Cumulative sum of top-10 exchanges.
display(exch_df.loc[:9])
cplpluti.plot_barplot(exch_df.set_index("Name")["Volume(24h)"].iloc[:9])

# %% [markdown]
# If we sort exchanges by last 24h volume, we see that lots of 'inferior' exchanges in top-10 (see # column that ranks by coinmarketcap exchange score).

# %%
# Another approach: sort only top-20 exchanges by Exchange score.
top20 = exch_df.sort_values(
    ["#"], ascending=True, ignore_index=True).loc[:19]
top20= top20.sort_values(
    ["Volume(24h)"], ascending=False, ignore_index=True
).loc[:9]
# Add 'relative volume': Exchange volume divided by the total volume from top-20 exchanges.
top10 = top20.loc[:9]#
top10["Relative_Volume"] = top10["Volume(24h)"]/top20["Volume(24h)"].sum()

# %%
# Cumulative sum of top-10 exchanges (selected from top-20 by Exchange Score).
display(top10)
cplpluti.plot_barplot(top10.set_index("Name")["Volume(24h)"])

# %%
# Relative volume of top-10 exchanges (selected from top-20 by Exchange Score).
display(top10)
cplpluti.plot_barplot(top10.set_index("Name")["Relative_Volume"])

# %%
# Another approach: sort by Exchange Score.
top10_rating = exch_df.sort_values(
    ["#"], ascending=True, ignore_index=True).loc[:9]

top10_rating = top10_rating.sort_values(
    ["Volume(24h)"], ascending=False, ignore_index=True
)

# %%
# Exchange volume of top-10 exchanges (by Exchange Score).
display(top10_rating)
cplpluti.plot_barplot(top10_rating.set_index("Name")["Volume(24h)"])

# %% [markdown]
# ## Cryptocurrencies

# %%
# Read .html file.
file_name_cc = "Cryptocurrency Prices, Charts And Market Capitalizations _ CoinMarketCap.html"
file_cc = pd.read_html(file_name_cc)
cc_df = file_cc[0]

# %%
# Clean up.
cc_df['Name'] = cc_df['Name'].str.findall('[^\d+]*').apply(lambda x: x[0])

cc_df['Market Cap'] = cc_df['Market Cap'].str.findall('[^$]*').apply(lambda x: x[3])
cc_df['Market Cap'] = cc_df['Market Cap'].str.replace(',', '')
cc_df["Market Cap"] = pd.to_numeric(cc_df["Market Cap"])

cc_df['Volume(24h)'] = cc_df['Volume(24h)'].astype('str').str.extractall('(\d+)').unstack().fillna('').sum(axis=1)
cc_df["Volume(24h)"] = pd.to_numeric(cc_df["Volume(24h)"])

# %%
# Select columns.
cc_df = cc_df[["#","Name", "Market Cap", "Volume(24h)"]]

# %%
# Cryptocurrencies by Market Cap.
display(cc_df.loc[:9])
cplpluti.plot_barplot(cc_df.set_index("Name")["Market Cap"].iloc[:9])

# %%
# Cryptocurrencies by last 24h volume.
display(cc_df.sort_values("Volume(24h)",ascending=False).iloc[:9])
cplpluti.plot_barplot(cc_df.set_index("Name").sort_values("Volume(24h)",ascending=False)["Volume(24h)"].iloc[:9])
