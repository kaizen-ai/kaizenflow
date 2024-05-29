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
import sqlite3
import pandas as pd
import numpy as np
import seaborn as sns
import itertools
import matplotlib.pyplot as plt
import os
import calendar
import datetime 
import zipfile
import re
import sklearn.model_selection as sms 
import scipy.stats
import sklearn.preprocessing 
import sklearn.impute
import boto3
import io 
import scipy.optimize as sopt
import warnings
import joblib 
import math
import statsmodels.api as sm
import statsmodels.formula.api as smf
import sklearn.metrics as skm
import helpers.haws as haws
import numpy as np
import pandas as pd
import scipy.stats as scps 
pd.set_option('display.max_columns', None)
warnings.filterwarnings("ignore")

# %%
# Initialize a session.
s3 = haws.get_service_resource(aws_profile='ck', service_name = 's3')

# Define the S3 bucket and the file location
bucket_name = 'cryptokaizen-data-test'
s3_file_path = 'kaizen_ai/soccer_prediction/model_output/glm_model_predictions.csv'

# Retrieve the CSV file from S3
obj = s3.Object(bucket_name, s3_file_path)
data = obj.get()['Body'].read().decode('utf-8')

# Load the CSV file content into a pandas DataFrame
poisson_df = pd.read_csv(io.StringIO(data))

# Display the first few rows of the DataFrame
print(poisson_df.head())

# %% [markdown]
# ### Paired t-test Random classifier-Poisson

# %%
# Normalize probabilities to sum to 1
poisson_df['prob_home_win'] /= (poisson_df['prob_home_win'] + poisson_df['prob_away_win'] + poisson_df['prob_draw'])
poisson_df['prob_away_win'] /= (poisson_df['prob_home_win'] + poisson_df['prob_away_win'] + poisson_df['prob_draw'])
poisson_df['prob_draw'] /= (poisson_df['prob_home_win'] + poisson_df['prob_away_win'] + poisson_df['prob_draw'])

# Determine predicted outcomes based on highest probability
poisson_df['predicted_poisson'] = poisson_df[['prob_home_win', 'prob_away_win', 'prob_draw']].idxmax(axis=1)
poisson_df['predicted_poisson'] = poisson_df['predicted_poisson'].apply(lambda x: 'home' 
                                                        if x == 'prob_home_win' 
                                                        else ('away' if x == 'prob_away_win' else 'draw'))

# Generate random predictions for the naive classifier
np.random.seed(42)  # For reproducibility
poisson_df['predicted_random'] = np.random.choice(['home', 'away', 'draw'], size=len(df))

# Define actual outcomes
def get_actual_outcome(row):
    if row['HS'] > row['AS']:
        return 'home'
    elif row['HS'] < row['AS']:
        return 'away'
    else:
        return 'draw'

poisson_df['actual_outcome'] = poisson_df.apply(get_actual_outcome, axis=1)

# Compute accuracy for Poisson model and random classifier
poisson_df['accuracy_poisson'] = (poisson_df['predicted_poisson'] == poisson_df['actual_outcome']).astype(int)
poisson_df['accuracy_random'] = (poisson_df['predicted_random'] == poisson_df['actual_outcome']).astype(int)

# Perform paired t-test
t_stat, p_value = scps.ttest_rel(poisson_df['accuracy_poisson'], poisson_df['accuracy_random'])

# Print results
print(f"Poisson Model Accuracy: {poisson_df['accuracy_poisson'].mean()}")
print(f"Random Classifier Accuracy: {poisson_df['accuracy_random'].mean()}")
print(f"Paired t-test: t-statistic = {t_stat}, p-value = {p_value}")

# %% [markdown]
# ### visualize the value counts of wins/losses/draw

# %%
# 1. Instances that home team won vs. away team won vs. draw
win_counts = df['actual_outcome'].value_counts()
plt.figure(figsize=(8, 6))
sns.barplot(x=win_counts.index, y=win_counts.values, palette='viridis')
plt.title('Home Team Wins vs Away Team Wins vs Draws')
plt.ylabel('Count')
plt.show()

# %% [markdown]
# ### Naive - Poisson

# %%
# Normalize probabilities to sum to 1
df['prob_home_win'] /= (df['prob_home_win'] + df['prob_away_win'] + df['prob_draw'])
df['prob_away_win'] /= (df['prob_home_win'] + df['prob_away_win'] + df['prob_draw'])
df['prob_draw'] /= (df['prob_home_win'] + df['prob_away_win'] + df['prob_draw'])

# Determine predicted outcomes based on highest probability
df['predicted_poisson'] = df[['prob_home_win', 'prob_away_win', 'prob_draw']].idxmax(axis=1)
df['predicted_poisson'] = df['predicted_poisson'].apply(lambda x: 'home' if x == 'prob_home_win' else ('away' if x == 'prob_away_win' else 'draw'))

# Generate predictions for the naive classifier (always predicting home win)
df['predicted_naive'] = 'home'

# Define actual outcomes
def get_actual_outcome(row):
    if row['HS'] > row['AS']:
        return 'home'
    elif row['HS'] < row['AS']:
        return 'away'
    else:
        return 'draw'

df['actual_outcome'] = df.apply(get_actual_outcome, axis=1)

# Compute accuracy for Poisson model and naive classifier
df['accuracy_poisson'] = (df['predicted_poisson'] == df['actual_outcome']).astype(int)
df['accuracy_naive'] = (df['predicted_naive'] == df['actual_outcome']).astype(int)

# Perform paired t-test
t_stat_poisson_naive, p_value_poisson_naive = scps.ttest_rel(df['accuracy_poisson'], df['accuracy_naive'])

# Print results
print(f"Poisson Model Accuracy: {df['accuracy_poisson'].mean()}")
print(f"Naive Classifier Accuracy: {df['accuracy_naive'].mean()}")
print(f"Paired t-test (Poisson vs. Naive): t-statistic = {t_stat_poisson_naive}, p-value = {p_value_poisson_naive}")
