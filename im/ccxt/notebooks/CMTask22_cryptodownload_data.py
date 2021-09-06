# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.4
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %%
# %load_ext autoreload
# %autoreload 2

import logging
from typing import List

import helpers.dbg as dbg
import helpers.env as henv
import helpers.printing as hprint
import helpers.s3 as hs3
import pandas as pd

import bs4
import urllib

# %%
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

# %%
WEBSITE_PREFIX = "https://www.cryptodatadownload.com/data/"
BINANCE_SYMBOL = "binance"
CURRENCY_SYMBOLS = ["BTC/USDT",
"ETH/USDT",
"BTC/USD",
"SOL/USDT",
"FIL/USDT",
"ETH/USD",
"XRP/USDT",
"FTM/USDT",
"DOGE/USDT",
"ADA/USDT",
"LINK/USDT",
"BTC/BUSD",
"BNB/USDT",
"AVAX/USDT",
"EOS/USDT"]


# %%
def get_download_page(exchange_symbol: str) -> str:
    download_url = WEBSITE_PREFIX + exchange_symbol
    u1 = urllib.request.urlopen(download_url)
    page_content = []
    for line in u1:
        page_content.append(line.decode("utf-8"))
    page_content = "".join(page_content)
    return page_content

def get_download_links(download_page_content: str,
                      period: str) -> List[str]:
    soup = bs4.BeautifulSoup(download_page_content)
    download_tags = soup.find_all(lambda tag: tag.name == "a" and "minute" in tag.text.lower())
    download_links = [urllib.parse.urljoin(WEBSITE_PREFIX, tag["href"]) for tag in download_tags]
    return download_links

def download_table_to_s3(download_links):
    # (TODO: Danya) make sure all column names are not alphanum.


# %% [markdown]
# ## Create directory in AWS

# %% [markdown]
# ## Checking insides

# %%
binance = pd.read_csv("https://www.cryptodatadownload.com/cdd/Binance_BTCUSDT_minute.csv")

# %%
binance.head()

# %%
binance = pd.read_csv("https://www.cryptodatadownload.com/cdd/Binance_BTCUSDT_minute.csv", skiprows=1)

# %%
binance.head()

# %%
binance.tail()

# %% [markdown]
# ## Parse Binance .csv links from download page

# %%
page_content = get_download_page("binance")

# %% [markdown]
# ### Get all links to minute data

# %%
minute_dl_links = get_download_links(page_content,
                                    "minute")

# %%
minute_dl_links

# %% [markdown]
# ## Save data to s3

# %%
df = pd.read_csv('https://www.cryptodatadownload.com/cdd/OCEANUSDT_Binance_futures_data_minute.csv', skiprows=1)

# %%
df

# %%
