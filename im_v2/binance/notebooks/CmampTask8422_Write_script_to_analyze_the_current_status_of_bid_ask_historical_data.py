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
# ## S3 Data Analysis Notebook
#
# This notebook analyzes historical bid-ask data downloaded using `amp/im_v2/binance/data/extract/download_historical_bid_ask.py` and stored in an S3 bucket. It retrieves and processes the data, and outputs a summary of the data availability and missing dates.
#

# %%
# %load_ext autoreload
# %autoreload 2
# %matplotlib inline

# %%
# Importing modules.
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List

import pandas as pd
from IPython.display import display

import helpers.haws as haws
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hprint as hprint
import helpers.hs3 as hs3

# %%
hdbg.init_logger(verbosity=logging.INFO)
log_level = logging.INFO

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %% [markdown]
# ## Configuration
#
# #### Update the configuration section with the appropriate parameters to customize the analysis.

# Configuration
config = {
    "stage": "preprod",
    "save_path_prefix": "binance/historical_bid_ask/",
}


# %% [markdown]
# ## Helper Functions
#
# #### The following functions help in listing and processing S3 objects.


# %%
# Extract the directory structure.
def extract_data_structure(objects):
    """
    Extract the directory structure and relevant data from the S3 objects.

    :param objects: S3 objects,
        e.g.
            ```
            [
                {
                    'Key': 'binance/historical_bid_ask/S_DEPTH/1000BONK_USDT/2023-05-27/data.tar.gz',
                    'LastModified': datetime.datetime(2024, 5, 30, 17, 12, 12, tzinfo=tzlocal()),
                    'ETag': '"d41d8cd98f00b204e9800998ecf8427e"',
                    'Size': 0,
                    'StorageClass': 'STANDARD'
                },
                {
                    'Key': 'binance/historical_bid_ask/S_DEPTH/1000BONK_USDT/2023-05-28/data.tar.gz',
                    'LastModified': datetime.datetime(2024, 5, 30, 17, 12, 12, tzinfo=tzlocal()),
                    'ETag': '"d41d8cd98f00b204e9800998ecf8427e"',
            ]
            ```
    """
    data_types = {}
    for obj in objects:
        key = obj["Key"]
        # Filter non-empty data.tar.gz files.
        if key.endswith("data.tar.gz") and obj["Size"] > 0:
            parts = key.split("/")
            date_part = parts[-2]
            date = datetime.strptime(date_part.split("_")[0], "%Y-%m-%d").date()
            symbol = parts[-3]
            data_type = parts[-4]
            #
            if data_type not in data_types:
                data_types[data_type] = {}
            #
            if symbol not in data_types[data_type]:
                data_types[data_type][symbol] = {"dates": [], "size": []}
            #
            data_types[data_type][symbol]["dates"].append(date)
            data_types[data_type][symbol]["size"].append(obj["Size"])

    return data_types


# %%
def group_consecutive_timestamps(
    timestamps: List[pd.Timestamp], threshold: timedelta = timedelta(days=1)
) -> List[List[pd.Timestamp]]:
    """
    Group consecutive timestamps for better readability. If the length of the
    list is 2, the first element in the list is the start timestamp and the
    second element is the end timestamp. If the length of the list is 1, then
    only a single timestamp is grouped.

    :param timestamps: List of pd.Timestamp objects to be grouped.
    :param threshold: Timedelta object representing the maximum allowed difference
                      between consecutive timestamps for them to be grouped together.
                      Defaults to 1 day.

    :return: List of lists of grouped timestamps. Each sublist contains either a single timestamp
             or two timestamps indicating the start and end of a consecutive period.

    Example:
        ```
        group_consecutive_timestamps([
            pd.Timestamp("2023-05-25"),
            pd.Timestamp("2023-05-27"),
            pd.Timestamp("2023-05-28"),
            pd.Timestamp("2023-05-30")
        ])
        ```
        Output:
        ```
        [
            [pd.Timestamp("2023-05-25")],
            [pd.Timestamp("2023-05-27"), pd.Timestamp("2023-05-28")],
            [pd.Timestamp("2023-05-30")]
        ]
        ```
    """
    # Sort the timestamps
    timestamps.sort()
    current_group = []
    groups = []
    for timestamp in timestamps:
        if not current_group:
            current_group.append(timestamp)
            continue
        # Check if the current timestamp is within the threshold of the last timestamp in the current group
        if (timestamp - current_group[-1]) <= threshold:
            current_group.append(timestamp)
        else:
            # Append the current group to the groups list
            if len(current_group) == 1:
                groups.append(current_group)
            else:
                groups.append([current_group[0], current_group[-1]])
            # Start a new group
            current_group = [timestamp]
    # Append the last group to the groups list
    if current_group:
        if len(current_group) == 1:
            groups.append(current_group)
        else:
            groups.append([current_group[0], current_group[-1]])
    return groups


# %%
def analyze_metadata(data_types: Dict[Dict, Any]) -> pd.DataFrame:
    """
    Analyze the metadata to find missing dates and generate a summary.
    """
    results = []
    for data_type, symbols in data_types.items():
        for symbol, data in symbols.items():
            min_date = min(data["dates"])
            max_date = max(data["dates"])
            all_dates = pd.date_range(start=min_date, end=max_date).date
            missing_dates = sorted(set(all_dates) - set(data["dates"]))
            missing_dates_groups = group_consecutive_timestamps(missing_dates)
            # We only want to verify downloaded data from `2023-05-27` as there was symantics change
            # by binance after this date.
            final_groups = []
            for group in missing_dates_groups:
                if pd.Timestamp(group[0]) <= pd.Timestamp("2023-05-27"):
                    continue
                final_groups.append(group)
            results.append(
                {
                    "data_type": data_type,
                    "symbol": symbol,
                    "min_date": min_date,
                    "max_date": max_date,
                    "missing_dates": final_groups,
                }
            )

    # Convert results to data frame.
    results_df = pd.DataFrame(results)
    return results_df


# %%
s3_client = haws.get_service_client(aws_profile="ck", service_name="s3")
bucket_name = hs3.get_s3_bucket_from_stage(stage=config["stage"])
prefix = config["save_path_prefix"]
# List objects in the S3 bucket under the specified prefix.
all_objects = haws.list_all_objects(s3_client, bucket_name, prefix)
_LOG.info(f"Length of all objects in s3 bucket is : {len(all_objects)}")
# Extract the directory structure.
data_types = extract_data_structure(all_objects)
# Analyze metadata.
results = analyze_metadata(data_types)
# Display the df.
display(results)

# %%
T_DEPTH_df = results[results["data_type"] == "T_DEPTH"]
display(T_DEPTH_df)

# %%
S_DEPTH_df = results[results["data_type"] == "S_DEPTH"]
display(S_DEPTH_df)

# %%
