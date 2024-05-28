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
import sklearn.model_selection 
import scipy.stats
import sklearn.preprocessing 
import sklearn.impute
import boto3
import io 
import scipy.optimize 
import warnings
import joblib 
import math
import statsmodels.api as sm
import statsmodels.formula.api as smf
import sklearn.metrics
import helpers.haws as haws
pd.set_option('display.max_columns', None)
warnings.filterwarnings("ignore")

# %% [markdown]
# Set the s3 credentials

# %%
# Initialize a session.
session = boto3.Session(profile_name='ck')
s3 = session.client('s3')

# Set the S3 bucket and dataset path.
s3_bucket_name = 'cryptokaizen-data-test'
s3_dataset_path = 'kaizen-ai/datasets/football/OSF_football/'

# Define the local directory to save the files.
local_directory = 'datasets/OSF_football'
os.makedirs(local_directory, exist_ok=True)

# Function to download files from S3.
def download_files_from_s3(bucket, prefix, local_dir):
    try:
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if 'Contents' not in response:
            print(f"No files found at s3://{bucket}/{prefix}")
            return
        for obj in response['Contents']:
            key = obj['Key']
            if key.endswith('.txt'):
                local_file_path = os.path.join(local_dir, os.path.basename(key))
                print(f"Downloading {key} to {local_file_path}")
                s3.download_file(bucket, key, local_file_path)
    except Exception as e:
        print(f"Error occurred: {e}")

# Call the function to download the files
download_files_from_s3(s3_bucket_name, s3_dataset_path, local_directory)

# Load the datasets into pandas dataframes
dataframes_3 = {}
for dirname, _, filenames in os.walk(local_directory):
    for filename in filenames:
        if filename.endswith(".txt"):
            file_key = filename.split('.')[0] + '_df'
            filepath = os.path.join(dirname, filename)
            print(f"Loading {filepath}")
            df = pd.read_csv(filepath, sep="\t", encoding="UTF-8")
            print(file_key, df.shape)
            df = df.drop_duplicates()
            dataframes_3[file_key] = df

print('Data imported')

# Verify the content of dataframes_3 dictionary
for key, df in dataframes_3.items():
    print(f"{key}: {df.shape}")

# Access the dataframes directly from the dictionary
ISDBv1_df = dataframes_3.get('ISDBv1_df')
ISDBv2_df = dataframes_3.get('ISDBv2_df')

# Print the shapes to confirm they are loaded correctly
print(f"ISDBv1_df shape: {ISDBv1_df.shape if ISDBv1_df is not None else 'Not found'}")
print(f"ISDBv2_df shape: {ISDBv2_df.shape if ISDBv2_df is not None else 'Not found'}")

# %%
ISDBv2_df.head()

# %% [markdown]
# Make the season into a single interger for simplicity

# %%
ISDBv2_df['season'] = ISDBv2_df['Sea'].apply(lambda x: int('20' + str(x)[:2]))
df = ISDBv2_df[ISDBv2_df['season'] >= 2009]
# Preprocess the dataset
# Preprocess the dataset
df['Date'] = pd.to_datetime(df['Date'], dayfirst=True)
df.sort_values(by='Date', inplace=True)
# Define the burn-in period and warm-up season removal
def remove_warmup_and_burnin(df, warmup_seasons, burnin_rounds=5):
    filtered_df = pd.DataFrame()
    leagues = df['Lge'].unique()

    for league in leagues:
        league_df = df[df['Lge'] == league]
        seasons = league_df['Sea'].unique()
        for season in seasons:
            season_df = league_df[league_df['Sea'] == season]
            if season == seasons[0] and season in warmup_seasons:
                continue
            season_df = season_df.iloc[burnin_rounds:]
            filtered_df = pd.concat([filtered_df, season_df])

    return filtered_df
warmup_seasons = {2009}
df_final = remove_warmup_and_burnin(df, warmup_seasons)
# Split the data into training, validation, and test sets
train_size = int(0.6* len(df_final))
val_size = int(0.2 * len(df_final))
train_df = df_final[:train_size]
val_df = df_final[train_size:train_size + val_size]
test_df = df_final[train_size + val_size:]

# %%
df_final.columns

# %% [markdown]
# # Double Precision Model (IGNORE)

# %% run_control={"marked": true}
# Check if the validation dataframe is empty
if val_df.empty:
    print("Validation dataframe is empty.")
else:
    print(f"Validation dataframe has {len(val_df)} records.")

# Check if the training dataframe is empty
if train_df.empty:
    print("Training dataframe is empty.")
else:
    print(f"Training dataframe has {len(train_df)} records.")

# Define the Double Poisson likelihood function with exponential time weighting
def double_poisson_log_likelihood(params, df, teams, alpha=0.0019):
    strength = dict(zip(teams, params[:-1]))
    home_advantage = params[-1]
    current_date = datetime.datetime.now()

    # Map team strengths to home and away teams
    home_teams = df['HT'].map(strength)
    away_teams = df['AT'].map(strength)
    home_goals = df['HS'].values
    away_goals = df['AS'].values
    t_deltas = (current_date - df['Date']).dt.days
    weights = np.exp(-alpha * t_deltas)

    # Calculate expected goals
    lambda_home = np.exp(home_teams.values - away_teams.values + home_advantage)
    lambda_away = np.exp(away_teams.values - home_teams.values)

    # Calculate log-likelihoods for home and away goals
    log_likelihood_home = home_goals * np.log(lambda_home) - lambda_home - np.log(np.vectorize(math.factorial)(home_goals))
    log_likelihood_away = away_goals * np.log(lambda_away) - lambda_away - np.log(np.vectorize(math.factorial)(away_goals))

    # Sum the weighted log-likelihoods
    total_log_likelihood = np.sum(weights * (log_likelihood_home + log_likelihood_away))

    return -total_log_likelihood  # Return negative log-likelihood to maximize it by minimizing

# Define the teams and initial parameters
teams = pd.unique(df[['HT', 'AT']].values.ravel('K'))
initial_strength = np.zeros(len(teams))
initial_home_advantage = 0.1

# Function to refit parameters after each league round
def refit_parameters(train_df, teams):
    # Initial guess for teams' strength.
    initial_strength = np.zeros(len(teams))
    # Initial guess for home advantage
    initial_home_advantage = 0.1  
    initial_params = np.concatenate([initial_strength, [initial_home_advantage]])

    print("Starting optimization with initial parameters")
    result = scipy.optimize.minimize(
        double_poisson_log_likelihood,
        x0=initial_params,
        args=(train_df, teams),
        method='L-BFGS-B',
        options={ 'disp': True}  
    )
    print("Optimization result:", result)
    if not result.success:
        print("Optimization failed.")
    return result.x

# Initial parameter fitting
params = refit_parameters(train_df, teams)

# Extract the optimized parameters
team_strengths = dict(zip(teams, params[:-1]))
home_advantage = params[-1]

print("Optimized Team Strengths:")
print(team_strengths)
print("Home Advantage:")
print(home_advantage)

# Function to predict match outcomes
def predict_match(home_team, away_team, team_strengths, home_advantage):
    lambda_home = np.exp(team_strengths[home_team] - team_strengths[away_team] + home_advantage)
    lambda_away = np.exp(team_strengths[away_team] - team_strengths[home_team])
    return lambda_home, lambda_away

# Function to calculate Ranked Probability Score (RPS)
def rps(predictions, actual):
    cumulative_preds = np.cumsum(predictions)
    cumulative_actual = np.cumsum(actual)
    return np.sum((cumulative_preds - cumulative_actual) ** 2) / (len(predictions) - 1)

# Function to evaluate the model
def evaluate_model(df, team_strengths, home_advantage):
    correct_predictions = 0
    total_predictions = 0
    total_rps = 0

    def calculate_metrics(row):
        nonlocal correct_predictions, total_predictions, total_rps
        home_team = row['HT']
        away_team = row['AT']
        home_goals = row['HS']
        away_goals = row['AS']

        # Check if team strengths are available
        if home_team not in team_strengths or away_team not in team_strengths:
            print(f"Missing strength for teams: {home_team}, {away_team}")
            return

        lambda_home, lambda_away = predict_match(home_team, away_team, team_strengths, home_advantage)
        predicted_home_goals = np.round(lambda_home)
        predicted_away_goals = np.round(lambda_away)

        if (predicted_home_goals == home_goals) and (predicted_away_goals == away_goals):
            correct_predictions += 1

        # Determine the maximum number of goals to dynamically size the result arrays
        max_goals = int(max(home_goals, away_goals, predicted_home_goals, predicted_away_goals)) + 1

        # Create actual result distribution
        actual_result = np.zeros(max_goals)
        actual_result[int(home_goals)] = 1
        actual_result[int(away_goals)] = 1

        # Create prediction distribution
        predictions = np.zeros(max_goals)
        predictions[int(predicted_home_goals)] = lambda_home
        predictions[int(predicted_away_goals)] = lambda_away

        total_rps += rps(predictions, actual_result)
        total_predictions += 1

    joblib.Parallel(n_jobs=-1)(joblib.delayed(calculate_metrics)(row) for idx, row in df.iterrows())

    # Check if total_predictions is zero to avoid ZeroDivisionError
    if total_predictions == 0:
        print("No predictions made. Total predictions is zero.")
        return 0, 0

    accuracy = correct_predictions / total_predictions
    average_rps = total_rps / total_predictions

    return accuracy, average_rps

# Evaluate the model on the validation set
val_accuracy, val_rps = evaluate_model(val_df, team_strengths, home_advantage)

print("Validation Accuracy:")
print(val_accuracy)
print("Validation Ranked Probability Score (RPS):")
print(val_rps)

# Evaluate the model on the test set
test_accuracy, test_rps = evaluate_model(test_df, team_strengths, home_advantage)

# Calculate confidence intervals for hit rate
hit_rate = test_accuracy
z = 1.96  # 95% confidence interval
hit_rate_std = np.sqrt((hit_rate * (1 - hit_rate)) / len(test_df))
confidence_interval = (hit_rate - z * hit_rate_std, hit_rate + z * hit_rate_std)

print("Test Accuracy:")
print(test_accuracy)
print("Test Ranked Probability Score (RPS):")
print(test_rps)
print("Hit Rate:")
print(hit_rate)
print("Confidence Interval:")
print(confidence_interval)


# %% [markdown]
# # GLM Model 

# %%
ISDBv2_df['season'] = ISDBv2_df['Sea'].apply(lambda x: int('20' + str(x)[:2]))
df = ISDBv2_df[ISDBv2_df['season'] >= 2009]
# Preprocess the dataset.
df['Date'] = pd.to_datetime(df['Date'], dayfirst=True)
df.sort_values(by='Date', inplace=True)
categorical_columns = ['HT', 'AT']
for col in categorical_columns:
    df[col] = df[col].astype('category')
# Ensure reproducibility.
random_state = 42
# Step 1: Split by team to ensure each team is represented in the train split.
teams = df['HT'].unique()
train_indices = []
test_indices = []
for team in teams:
    team_df = df[df['HT'] == team]
    train_team, test_team = sklearn.model_selection.train_test_split(
        team_df, 
        test_size=0.2, 
        random_state=random_state
    )
    train_indices.extend(train_team.index)
    test_indices.extend(test_team.index)
# Create train and test DataFrames.
train_df = df.loc[train_indices]
test_df = df.loc[test_indices]

def unravel_dataset(df) -> pd.DataFrame():
    """
    Unravel the dataset by creating one entry for each row as team-opponent pair. 
    """
    home_df = df[['Date', 'Sea', 'Lge', 'HT', 'AT', 'HS']].copy()
    home_df.rename(columns={'HT': 'team', 'AT': 'opponent', 'HS': 'goals'}, inplace=True)
    home_df['is_home'] = 1
    away_df = df[['Date', 'Sea', 'Lge', 'HT', 'AT', 'AS']].copy()
    away_df.rename(columns={'AT': 'team', 'HT': 'opponent', 'AS': 'goals'}, inplace=True)
    away_df['is_home'] = 0
    unraveled_df = pd.concat([home_df, away_df], ignore_index=True)
    return unraveled_df

# Unravel the training dataset.
unraveled_train_df = unravel_dataset(train_df)
# Unravel the test dataset.
unraveled_test_df = unravel_dataset(test_df)

# %%
unraveled_train_df.head()


# %% [markdown]
# Create a representative sample for easier handling.

# %%
def representative_sample(df, sample_size) -> pd.DataFrame():
    """
    Function to perform representative sampling to ensure each team 
    is represented.
    param: df: Input dataframe for sampling.
    param: sample_size: Size of the extracted sample (output dataframe).
    return: sampled_df: Sampled dataframe.
    """
    teams = df['team'].unique()
    samples_per_team = sample_size // len(teams)
    sampled_df = pd.DataFrame()
    for team in teams:
        team_df = df[df['team'] == team]
        team_sample = team_df.sample(n=min(samples_per_team, len(team_df)), random_state=1)
        sampled_df = pd.concat([sampled_df, team_sample])
    # Additional random sampling to fill the remaining sample size
    remaining_sample_size = sample_size - len(sampled_df)
    if remaining_sample_size > 0:
        additional_sample = df.drop(sampled_df.index).sample(n=remaining_sample_size, random_state=1)
        sampled_df = pd.concat([sampled_df, additional_sample])
    return sampled_df

# Sample 20% of the training data.
sample_size = int(0.2 * len(unraveled_train_df))
# Perform representative sampling on the training set.
sampled_train_df = representative_sample(unraveled_train_df, sample_size)
sampled_train_df.head()

# %% [markdown]
# Describe the sampled DataFrame.

# %%
print("NaN values per column:")
print(sampled_train_df.isna().sum())

numeric_cols = sampled_train_df.select_dtypes(include=[np.number]).columns
print("\nInfinite values per numeric column:")
for col in numeric_cols:
    num_infs = np.isinf(sampled_train_df[col]).sum()
    print(f"{col}: {num_infs}")
print(sampled_train_df.dtypes)
print(sampled_train_df.describe(include='all'))


# %%
def ensure_finite_weights(df) -> pd.DataFrame():
    """
    Function to ensure weights are finite.
    """
    # Adding a small constant to goals to avoid log(0).
    df['goals'] = df['goals'].apply(lambda x: x + 1e-9 if x == 0 else x)
    # Check if there are any infinite or NaN weights and handle them
    if df.isna().sum().sum() > 0:
        print("NaN values found in the data. Removing rows with NaNs.")
        df.dropna(inplace=True)
    if np.isinf(df.select_dtypes(include=[np.number])).sum().sum() > 0:
        print("Infinite values found in the data. Removing rows with Infs.")
        df = df[~np.isinf(df.select_dtypes(include=[np.number])).any(1)]
    return df

# Ensure weights are finite in the sampled training data.
sampled_train_df = ensure_finite_weights(sampled_train_df)
# Create the formula to include team offensive and opponent defensive strengths and home advantage.
formula = 'goals ~ C(team) + C(opponent) + is_home'
# Fit the Poisson regression model.
poisson_model = smf.glm(formula=formula, data=sampled_train_df, family=sm.families.Poisson()).fit(maxiter=10)
# Display the summary of the model.
print(poisson_model.summary())

# %% [markdown]
# Generate Predictions

# %% run_control={"marked": true}
# Predict the expected goals for home and away teams in the test set.
unraveled_test_df['predicted_goals'] = poisson_model.predict(unraveled_test_df)
unraveled_test_df

# %%
# Split the dataframe into home and away rows.
home_df = unraveled_test_df[unraveled_test_df['is_home'] == 1].copy()
away_df = unraveled_test_df[unraveled_test_df['is_home'] == 0].copy()
# Rename columns for merging
home_df.rename(columns={'team': 'HT', 'opponent': 'AT', 'goals': 'HS', 'predicted_goals': 'Lambda_HS'}, inplace=True)
away_df.rename(columns={'team': 'AT', 'opponent': 'HT', 'goals': 'AS', 'predicted_goals': 'Lambda_AS'}, inplace=True)
# Merge the home and away dataframes
merged_df = pd.merge(home_df, away_df, on=['Date', 'Sea', 'Lge', 'HT', 'AT'], suffixes=('_home', '_away'))
# Select and reorder columns for the final dataframe
test_df = merged_df[['Date', 'Sea', 'Lge', 'HT', 'AT', 'HS', 'AS', 'Lambda_HS', 'Lambda_AS']]
# Display the resulting dataframe
print(test_df.head())

# %% [markdown]
# Evaluate

# %%
# Round off the predicted goals to integers
test_df['Lambda_HS'] = test_df['Lambda_HS'].round().astype(int)
test_df['Lambda_AS'] = test_df['Lambda_AS'].round().astype(int)

# Define the 
def calculate_match_outcome_probabilities(row):
    """
    Function to calculate match outcome probabilities.
    """
    max_goals = 10  
    home_goals_probs = [np.exp(-row['Lambda_HS']) * row['Lambda_HS']**i / np.math.factorial(i) for i in range(max_goals)]
    away_goals_probs = [np.exp(-row['Lambda_AS']) * row['Lambda_AS']**i / np.math.factorial(i) for i in range(max_goals)]
    prob_home_win = 0
    prob_away_win = 0
    prob_draw = 0
    for i in range(max_goals):
        for j in range(max_goals):
            prob = home_goals_probs[i] * away_goals_probs[j]
            if i > j:
                prob_home_win += prob
            elif i < j:
                prob_away_win += prob
            else:
                prob_draw += prob
    return pd.Series({
        'prob_home_win': prob_home_win,
        'prob_away_win': prob_away_win,
        'prob_draw': prob_draw
    })
# Apply the function to the test set
probabilities = test_df.apply(calculate_match_outcome_probabilities, axis=1)
test_df = pd.concat([test_df, probabilities], axis=1)
# Display the test set with probabilities
print(test_df.head())
# Predict the outcomes based on probabilities
test_df['predicted_outcome'] = np.where(test_df['prob_home_win'] > test_df['prob_away_win'], 'home_win',
                                        np.where(test_df['prob_away_win'] > test_df['prob_home_win'], 
                                                 'away_win', 'draw'))
# Calculate actual outcomes for comparison
test_df['actual_outcome'] = np.where(test_df['HS'] > test_df['AS'], 'home_win',
                                     np.where(test_df['HS'] < test_df['AS'], 'away_win', 'draw'))
# Calculate accuracy
accuracy = sklearn.metrics.accuracy_score(test_df['actual_outcome'], test_df['predicted_outcome'])
print("Model Accuracy on Test Set:", accuracy)

# %%
