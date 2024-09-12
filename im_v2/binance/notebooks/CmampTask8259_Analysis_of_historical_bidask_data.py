# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.15.2
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # DESCRIPTION
#
# In this notebook we will load the historical bid ask data, and do some basic analysis.

# %%
import logging
import os
import tarfile

import boto3
import numpy as np
import pandas as pd

import core.finance.resampling as cfinresa
import core.plotting.boxplot as cploboxp
import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hpandas as hpandas
import helpers.hprint as hprint
import im_v2.common.data.client.im_raw_data_client as imvcdcimrdc
import im_v2.common.data.qa.dataset_validator as imvcdqdava
import im_v2.common.data.qa.qa_check as imvcdqqach
import im_v2.common.universe.universe as imvcounun

# Initialize the S3 client
s3 = boto3.client('s3')

# %%
hdbg.init_logger(verbosity=logging.INFO)
log_level = logging.INFO

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()


# %%
def get_historical_data(currency_pair: str, date: pd.Timestamp, data_type: str = "T_DEPTH"):
    """
    Copy the tar file from S3 to local, load the tar file and return.

    :param currency_pair: symbol name for the bid_ask data eg: BTC_USDT
    :param date: download data date
    :param data_type: 'T_DEPTH' or 'S_DEPTH'
    """
    # Specify the bucket name and object key of the .tar.gz file on S3
    date_str = date.strftime('%Y-%m-%d')
    bucket_name = 'cryptokaizen-data-test'
    object_key = f'sonaal_test/binance/historical_bid_ask/{data_type}/{currency_pair}/{date_str}/data.tar.gz'
    # Specify the local directory where you want to extract the contents
    local_dir = 'temp/'
    # Create the local directory if it doesn't exist
    os.makedirs(local_dir, exist_ok=True)
    # Download the .tar.gz file from S3 to a local file
    local_file = os.path.join(local_dir, 'file.tar.gz')
    s3.download_file(bucket_name, object_key, local_file)
    # Extract the contents of the .tar.gz file
    with tarfile.open(local_file, 'r:gz') as tar:
        tar.extractall(local_dir)
    # Optionally, delete the downloaded .tar.gz file
    os.remove(local_file)
    symbol = currency_pair.replace("_","")
    if data_type == "T_DEPTH":
        snap_csv = pd.read_csv(f"temp/{symbol}_T_DEPTH_2024-04-04_depth_snap.csv")
        update_csv = pd.read_csv(f"temp/{symbol}_T_DEPTH_2024-04-04_depth_update.csv")
        return snap_csv, update_csv
    elif data_type == "S_DEPTH":
        s_depth = pd.read_csv(f"temp/{symbol}_S_DEPTH_2024-04-04.csv")
        return s_depth


# %%
start_timestamp = pd.Timestamp("2024-04-04T00:00:00+00:00")
end_timestamp = pd.Timestamp("2024-04-04T00:20:00+00:00")
currency_pair = "BNB_USDT"

# %%
snap_csv, update_csv = get_historical_data(currency_pair, start_timestamp)
snap_csv = snap_csv.rename(columns={"symbol" : "currency_pair"})
update_csv = update_csv.rename(columns={"symbol" : "currency_pair"})

# %% [markdown]
# ## Sanity checks Snapshot CSV

# %%
snap_csv.head()

# %%
# max of ask price < min of bid price
snap_csv[snap_csv["side"] == "a"]["price"].min() > snap_csv[snap_csv["side"] == "b"]["price"].max()

# %%
sorted(snap_csv[snap_csv["side"] == "a"]["price"])[:20]

# %%
sorted(snap_csv[snap_csv["side"] == "b"]["price"])[::-1][:20]

# %%
bids_snap = snap_csv[snap_csv['side'] == 'b'].groupby('price')['qty'].sum().sort_index(ascending=False)
ask_snap = snap_csv[snap_csv['side'] == 'a'].groupby('price')['qty'].sum().sort_index()

# %%
bids_snap

# %%
ask_snap

# %%
cploboxp.plot_bid_ask_price_levels(bids_snap, ask_snap)

# %%
snap_csv["trans_id"].value_counts()

# %%
snap_csv["price"].value_counts()

# %%
snap_csv[["price", "qty"]].describe()

# %% [markdown]
# ## Sanity Checks Update CSV

# %%
update_csv.head()

# %%
update_csv["update_type"].value_counts()

# %%
update_csv[["price", "qty"]].describe()

# %% [markdown]
# ## QA Check
#
# Perform basic QA to check:
# 1. Gaps in data
# 2. NaN values in data

# %%

qa_check_list = [
    imvcdqqach.GapsInTimeIntervalBySymbolsCheck(
        start_timestamp, end_timestamp, "1T", align=True
    ),
    imvcdqqach.NaNChecks(),
]
dataset_validator = imvcdqdava.DataFrameDatasetValidator(qa_check_list)

# %%
try:
    dataset_validator.run_all_checks([update_csv])
except Exception as e:
    # Pass information about success or failure of the QA
    #  back to the task that invoked it.
    data_qa_outcome = str(e)
    raise e

# %%
