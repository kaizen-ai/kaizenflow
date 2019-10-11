# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.2'
#       jupytext_version: 1.2.4
#   kernelspec:
#     display_name: Python [conda env:.conda-p1_develop] *
#     language: python
#     name: conda-env-.conda-p1_develop-py
# ---

# %% [markdown]
# # Imports

# %%
# %load_ext autoreload
# %autoreload 2

import os

from matplotlib import pyplot as plt
import pandas as pd
import seaborn as sns
sns.set()

from tqdm.autonotebook import tqdm

import oil.utils.datetime_utils as du
import vendors.first_rate.utils as fru

# %matplotlib inline

# %%
from pylab import rcParams
rcParams['figure.figsize'] = (20, 5)

# %%
import infra.helpers.telegram_notify.telegram_notify as tgn
tn = tgn.TelegramNotify()


# %%
def find_price_col_bug(df):
    """
    Check that column names for ['open', 'high', 'low', 'close', 'settle']
    columns correspond to one of those columns (they can be shifted)
    """
    price_cols = df.columns.intersection(
        ['open', 'high', 'low', 'close', 'settle'])
    mean_price = df[price_cols].mean()
    col_name_bug = False
    for price_col in price_cols:
        mean_other_cols = mean_price.loc[mean_price.index.drop(
            price_col)].mean()
        if not (0.98 < (mean_price.loc[price_col] / mean_other_cols) < 1.2):
            col_name_bug = True
    return col_name_bug


def verify(pq_path):
    equity = pd.read_parquet(pq_path)
    equity['timestamp'] = pd.to_datetime(equity['timestamp'], utc=True)
    summary_series = pd.Series(index=summary_cols)
    summary_series.loc['start_date'] = equity['timestamp'].iloc[0]
    summary_series.loc['end_date'] = equity['timestamp'].iloc[-1]
    summary_series.loc['n_rows'] = len(equity)
    summary_series.loc['price_col_bug'] = find_price_col_bug(equity)
    # check timestamps and missing days
    if 'timestamp' not in equity.columns:
        summary_series.loc['missing_timestamp_col'] = True
        bdays = pd.date_range(equity['timestamp'].iloc[0],
                              equity['timestamp'].iloc[-1],
                              freq=du.CBD)
        n_missing_days = len(bdays) - len(equity)
        summary_series.loc['n_missing_market_days'] = n_missing_days
    else:
        summary_series.loc['missing_timestamp_col'] = False
        bdays = pd.date_range(equity['timestamp'].iloc[0],
                              equity['timestamp'].iloc[-1],
                              freq=du.CBD,
                              tz=equity['timestamp'].iloc[0].tz)
        missing_bdays = bdays.difference(equity['timestamp'].dt.date)
        summary_series.loc['n_missing_market_days'] = len(missing_bdays)
    return summary_series


# %%
PQ_DIR = '/data/first_rate/pq'

# %% [markdown]
# # Get file list

# %%
pq_files = []
for category_dir in os.listdir(PQ_DIR):
    category_dir_path = os.path.join(PQ_DIR, category_dir)
    for file_name in os.listdir(category_dir_path):
        file_path = os.path.join(category_dir_path, file_name)
        pq_files.append(file_path)

# %%
len(pq_files)

# %% [markdown]
# # Collect a summary

# %%
summary_cols = [
    'start_date', 'end_date', 'n_missing_market_days', 'n_rows',
    'missing_timestamp_col', 'price_col_bug'
]

# %%
summary = pd.DataFrame(columns=summary_cols, index=pq_files)
summary.index.name = 'file_name'

# %%
# %%time
for pq_path in tqdm(pq_files):
    summary_equity = verify(pq_path)
    summary.loc[pq_path] = summary_equity

# %%
summary.to_csv('/data/first_rate/file_summary.csv', index=0)

# %%
tn.notify("Collected stats for each pq")

# %%
tn.notify(summary.head())

# %% [markdown]
# # Stats

# %%
summary.head()

# %%
summary['missing_timestamp_col'].sum()

# %%
summary['price_col_bug'].sum()

# %%
summary['n_missing_market_days'] / summary['n_rows']

# %%
summary['n_rows'].sort_values(ascending=False).head(10)

# %%
sns.distplot(summary['n_rows'])
plt.title('Number of rows per equity')
plt.show()

# %%
sns.distplot(summary['n_missing_market_days'] / summary['n_rows'])
plt.title('Proportion of missing market days per equity')
plt.show()

# %%
top_missing_pct = (summary['n_missing_market_days'] /
                   summary['n_rows']).sort_values(ascending=False)

# %%
top_missing_pct.head(20)

# %%
summary.loc[top_missing_pct.head(20).index]

# %%
