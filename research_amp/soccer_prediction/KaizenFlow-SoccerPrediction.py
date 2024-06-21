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

# %%
import logging

import pandas as pd
import numpy as np

import core.finance as cofinanc
import dataflow.core as dtfcore
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hpandas as hpandas
import helpers.hprint as hprint

import research_amp.soccer_prediction.utils as rasoprut

# %%
hdbg.init_logger(verbosity=logging.DEBUG)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()


# %%
def preprocess_data(df: pd.DataFrame()) -> pd.DataFrame():
    """
    Preprocess the loaded ISDB dataframe of interest.
        - Filter and select match from seasons starting from 2009.
        - Convert column formats.
        - Add epsilon = 0.5 to scores with value as `0` to avoid log(0).
        - Check for NaN and infinite values and drop the rows.
    
    :param df: Input DataFrame. 
    :return: Preprocessed DataFrame.
    """
    df["season"] = df["Sea"].apply(lambda x: int("20" + str(x)[:2]))
    filtered_df = df[df["season"] >= 2009]
    # Preprocess the dataset.
    df["Date"] = pd.to_datetime(df["Date"], dayfirst=True)
    df.sort_values(by="Date", inplace=True)
    # Covert the categorical columns to category type. 
    categorical_columns = ["HT", "AT"]
    for col in categorical_columns:
        filtered_df[col] = filtered_df[col].astype("category")
    # Adding a small constant to goals to avoid log(0).
    columns = ['AS', 'HS']
    epsilon = 0.0
    for column in columns:    
        filtered_df[column] = filtered_df[column].apply(lambda x: x + epsilon if x == 0 else x)
        # Check if there are any infinite or NaN weights and handle them.
        if filtered_df.isna().sum().sum() > 0:
            _LOG.debug("NaN values found in the data. Removing rows with NaNs.")
            filtered_df.dropna(inplace=True)
        if filtered_df.isin([-np.inf, np.inf]).sum().sum() > 0:
            _LOG.debug("Infinite values found in the data. Removing rows with Infs.")
            filtered_df = filtered_df[~np.isinf(filtered_df.select_dtypes(include=[np.number])).any(1)]
    # Return the preprocessed DataFrame.
    return filtered_df


# %%
# Define the S3 Buckets, dataset path and local directory for download.
bucket = "cryptokaizen-data-test"
dataset_path = "kaizen_ai/soccer_prediction/datasets/OSF_football/"
local_dir = "datasets/OSF_football"
# Download data from S3.
rasoprut.download_data_from_s3(
    bucket_name=bucket, dataset_path=dataset_path, local_path=local_dir
)
# Load the data from S3 into pandas dataframe objects.
dataframes = rasoprut.load_data_to_dataframe(local_path=local_dir)

# %%
# Access the dataframes directly from the dictionary.
ISDBv1_df = dataframes.get("ISDBv1_df")
ISDBv2_df = dataframes.get("ISDBv2_df")
# Preprocess the selected dataframe (ISDBv2_df).
preprocessed_df = preprocess_data(ISDBv2_df)
# preprocessed_df.set_index('Date', inplace=True)

# %%
preprocessed_df[preprocessed_df["AT"] == 'FC Barcelona']


# %%
def get_home_team(df, team):
    df = df[df["HT"] == team]
    df["is_home"] = 1
    df = df.rename(columns={"AT" : "opponent", "HS" : "goals_scored", "AS": "goals_scored_by_opponent"})
    df = df.drop(["HT"], axis=1)
    return df.to_dict(orient='records')

def get_away_team(df, team):
    df = df[df["AT"] == team]
    df["is_home"] = 0
    df = df.rename(columns={"HT" : "opponent", "AS" : "goals_scored", "HS": "goals_scored_by_opponent"})
    df = df.drop(["AT"], axis=1)
    # Define the mapping to flip 'W' and 'L'
    flip_mapping = {'W': 'L', 'L': 'W'}
    # Apply the mapping to the 'WDL' column
    df['WDL'] = df['WDL'].replace(flip_mapping)
    df['GD'] = df['GD'].apply(lambda x : -x)
    return df.to_dict(orient='records')

get_away_team(preprocessed_df, 'FC Barcelona')

# %%
teams = set(preprocessed_df["HT"].to_list() +  preprocessed_df["AT"].to_list())
data = {}
teams = ['FC Barcelona', 'Real Madrid']
for team in teams:
    data[team] = []
    data[team].extend(get_home_team(preprocessed_df, team))
    data[team].extend(get_away_team(preprocessed_df, team))


# %%
data

# %%
# Flatten the dictionary of lists of dictionaries to create a list of DataFrames
flattened_data = {
    (outer_key, inner_key): [d.get(inner_key, None) for d in outer_list]
    for outer_key, outer_list in data.items()
    for inner_key in {k for d in outer_list for k in d.keys()}
}

# Convert the flattened dictionary to a DataFrame
df = pd.DataFrame(flattened_data)

# Sort the columns to ensure the outer keys are grouped together
df = df.sort_index(axis=1, level=0)
# Switch outer and inner levels
df.stack(level=0).unstack()

# %%
# `nid` is short for "node id"
nid = "df_data_source"
df_data_source = dtfcore.DfDataSource(nid, preprocessed_df)

# %%
df_out_fit = df_data_source.fit()["df_out"]
_LOG.debug(hpandas.df_to_str(df_out_fit))

# %%
"""
  is_home              GS
Manta FC

"""


# %%
def unravel_df(df: pd.DataFrame()) -> pd.DataFrame():
    """    
    Unravel the dataset by creating two entries for each row as team-opponent
    pair.
    
    :param df: Input dataframe.
    :return: unraveled dataframe.
    """
    # Create entry for home team `HT`.
    home_df = df[["Sea", "Lge", "HT", "AT", "HS"]].copy()
    home_df.rename(
                columns={"HT": "team", "AT": "opponent", "HS": "goals"}, inplace=True
                  )
    home_df["is_home"] = 1
    # Create entry for away team `AT`.
    away_df = df[["Sea", "Lge", "HT", "AT", "AS"]].copy()
    away_df.rename(
                columns={"AT": "team", "HT": "opponent", "AS": "goals"}, inplace=True
                  )
    away_df["is_home"] = 0
    # Concatenate the two splits.
    unraveled_df = pd.concat([home_df, away_df], ignore_index=True)
    # return the unraveled dataframe.
    return unraveled_df


# %%
# Prepare a DataFlow transformer node to compute percentage returns
#  and only return percentage returns.
nid = "unravel_dataset"
unravel_node_v2 = dtfcore.GroupedColDfToDfTransformer(
    nid,
    transformer_func=unravel_df,
    in_col_groups=[('Sea', 'Lge', 'HT', 'AT', 'HS', 'AS', 'GD', 'WDL', 'season')],
    out_col_group=(),
)

# %%
unravel_node_v2.fit(preprocessed_df)["df_out"]

# %%
