# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.2'
#       jupytext_version: 1.2.3
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Imports

# %%
# %load_ext autoreload
# %autoreload 2

import time

import numpy as np
import pandas as pd
import pickle

from tqdm.autonotebook import tqdm

# import oil.utils.Task268_download_metadata_from_CME as t268

# %%
import logging

_log = logging.getLogger()
_log.setLevel(logging.INFO)

# %%
# import oil.utils.helpers as helpers
# helpers.init_logger(logging.INFO)

# %%
from pylab import rcParams
rcParams['figure.figsize'] = (20, 5)


# %%
# import infra.helpers.telegram_notify.telegram_notify as tg

# tgn = tg.TelegramNotify()

# %%
def get_squash_cols(df):
    squash_cols = []
    for col_name in df.columns:
        if isinstance(col_name, int):
            if col_name > 0:
                squash_cols.append(col_name)
    return squash_cols


def squash_cols(df, cols):
    if cols == [1]:
        squashed_series = df[1]
    else:
        squashed_series = df[cols].fillna('').apply(" ".join, axis=1)
    return squashed_series


def add_links_as_rows(df):
    link_df = df[['link_1']].dropna()
    link_df.columns = [1]
    series = pd.concat([df, link_df.rename('{} Link'.format)])[[1]]
    return series


def get_row(df, idx):
    df = df.copy()
    df[1] = squash_cols(df, get_squash_cols(df))
    df = df[[1, 'link_1']]
    tr_df = add_links_as_rows(df).T
    tr_df.index = [idx]
    return tr_df


def rename_duplicate_cols(df):
    df = df.copy()
    dupe_mask = df.columns.duplicated(keep='first')
    duped_col_names = [
        f"{col_name}_{i}" for i, col_name in enumerate(df.columns[dupe_mask])
    ]
    new_index = np.array(df.columns)
    new_index[dupe_mask] = duped_col_names
    df.columns = new_index
    return df


# %% [markdown]
# # Read csv

# %%
pse = pd.read_csv('/data/prices/product_slate_export_20190904.csv')

# %%
pse.head()

# %%
pse.shape

# %% [markdown]
# # Extract HTMLs

# %%
name_link = pse[['Product Name', 'product_link']].copy()

# %%
name_link = name_link.rename(columns={
    'Product Name': 'product_name'
}).set_index('product_name')

# %%
name_link.head()

# %%
name_link.shape

# %%
np.unique(name_link.index).shape

# %% [markdown]
# # Load

# %%
product_names_htmls = {}
for name, link in tqdm(name_link.iterrows(), total=len(name_link)):
    time.sleep(1)
    product_names_htmls[name] = t268._extract(link[0])

# %%
# tgn.notify('Loaded htmls')

# %%
product_names_htmls['Eurodollar Futures']

# %% [markdown]
# # Dump

# %%
with open('/data/prices/Task268_contract_specs_20190904.pkl', 'wb') as fout:
    pickle.dump(product_names_htmls, fout)

# %% [markdown]
# # Add contract specs as rows

# %% [markdown]
# ## Read the data

# %%
pse = pd.read_csv('/data/prices/product_slate_export_20190904.csv')

# %%
with open('/data/prices/Task268_contract_specs_20190904.pkl', 'rb') as fin:
    product_names_htmls = pickle.load(fin)

# %%
pse.head()

# %%
product_names_htmls['Eurodollar Futures']

# %% [markdown]
# ## Investigate the data

# %%
pd.Series([
    contract_spec.shape[0] if contract_spec is not None else None
    for contract_spec in product_names_htmls.values()
]).value_counts()

# %%
pd.Series([
    contract_spec.shape[1] if contract_spec is not None else None
    for contract_spec in product_names_htmls.values()
]).value_counts()

# %%
for contract_spec in product_names_htmls.values():
    if contract_spec is not None:
        if contract_spec.shape[1] > 3:
            display(contract_spec)

# %%
for contract_spec in product_names_htmls.values():
    if contract_spec is not None:
        if contract_spec.shape[1] == 5:
            display(contract_spec)

# %%
sum([
    'link_1' in contract_spec.columns if contract_spec is not None else True
    for contract_spec in product_names_htmls.values()
])

# %%
len(product_names_htmls)

# %% [markdown]
# ## Add contract specs

# %%
product_names_htmls = {
    product_name: html.set_index(0) if html is not None else None
    for product_name, html in product_names_htmls.items()
}

# %%
product_names_htmls['Eurodollar Futures']

# %%
html_rows = [
    get_row(html, product_name) if html is not None else pd.DataFrame(
        index=[product_name])
    for product_name, html in product_names_htmls.items()
]

# %%
html_rows[0]

# %%
html_rows = [
    get_row(html, product_name) if html is not None else pd.DataFrame(
        index=[product_name])
    for product_name, html in product_names_htmls.items()
]

# %%
html_rows[8]

# %%
rename_duplicate_cols(html_rows[8])

# %%
html_rows = [rename_duplicate_cols(html) for html in html_rows]

# %%
html_rows_df = pd.concat(html_rows, sort=False)

# %%
html_rows_df.head()

# %%
pse_with_specs = pse.merge(html_rows_df,
                           left_on='Product Name',
                           right_index=True)

# %%
pse_with_specs.to_csv(
    '/data/prices/product_slate_export_with_contract_specs_20190905.csv', index=0)

# %%
