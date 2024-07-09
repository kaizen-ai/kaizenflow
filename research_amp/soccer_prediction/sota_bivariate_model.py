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
import warnings

import numpy as np
import pandas as pd

import helpers.hdbg as hdbg
import research_amp.soccer_prediction.models as rasoprmo
import research_amp.soccer_prediction.preproccesing as rasoprpr
import research_amp.soccer_prediction.utils as rasoprut
from scipy.special import factorial
from scipy.optimize import minimize


pd.set_option("display.max_columns", None)
warnings.filterwarnings("ignore")

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
data = dataframes["ISDBv2_df"]


# %%
# Convert date column to datetime
data['Date'] = pd.to_datetime(data['Date'], dayfirst=True)
# Define half-life period (e.g., 180 days)
half_life_period = 180
# Calculate the age of each match in days (relative to the most recent match)
data['Days_Ago'] = (data['Date'].max() - data['Date']).dt.days
# Calculate time weights
data['Time_Weight'] = 0.5 ** (data['Days_Ago'] / half_life_period)
# Generate unique team identifiers.
teams = pd.Series(data['HT'].tolist() + data['AT'].tolist()).unique()
team_to_id = {team: idx for idx, team in enumerate(teams)}
# Map teams to unique identifiers.
data['HT_id'] = data['HT'].map(team_to_id)
data['AT_id'] = data['AT'].map(team_to_id)
# Display the first few rows of the dataset
print(data.head())


# %%
def bivariate_poisson_log_likelihood(params, data):
    c, h, rho, *strengths = params
    log_likelihood = 0 
    for _, row in data.iterrows():
        i, j, goals_i, goals_j, time_weight = row['HT_id'], row['AT_id'], row['HS'], row['AS'], row['Time_Weight']
        lambda_i = np.exp(c + strengths[i] + h)
        lambda_j = np.exp(c + strengths[j] - h)
        cov = rho * np.sqrt(lambda_i * lambda_j)
        # Calculate joint probability
        joint_prob = 0
        for k in range(min(goals_i, goals_j) + 1):
            P_goals_i = (lambda_i**goals_i * np.exp(-lambda_i)) / factorial(goals_i)
            P_goals_j = (lambda_j**goals_j * np.exp(-lambda_j)) / factorial(goals_j)
            joint_prob = P_goals_i * P_goals_j
        log_likelihood += time_weight * np.log(joint_prob)
    return -log_likelihood 


# %%
# Number of teams
num_teams = len(teams)
# Initial parameters: [c, h, rho, *strengths]
initial_params = [0, 0, 0.1] + [1] * num_teams

# %%
# Select the data for the league and season.
final_data = data[(data['Lge'] == 'ENG5') & 
                     ((data['Sea'] == '07-08') | 
                      (data['Sea'] == '06-07') | 
                      (data['Sea'] == '08-09'))]
# Set optimization options.
options = {
    'maxiter': 10,  
    'disp': True      
}
# Optimize parameters using the BFGS algorithm with options.
result = minimize(bivariate_poisson_log_likelihood, initial_params, args=(final_data.iloc[:552],), method='L-BFGS-B', options=options)
optimized_params = result.x
print("Optimized Parameters:", optimized_params)

# %%
optimized_params[4]


# %%
def calculate_match_outcomes(df, params, *,
    max_goals: int = 10,
    apply_dixon_coles: bool = False,
    rho: float = -0.2,) -> pd.DataFrame:
    """
    Calculate match outcome probabilities.
    """
    c, h, rho, *strengths = params 
    # Calculate Lambda_HS and Lambda_AS for each row in the dataframe
    df["Lambda_HS"] = np.exp(c + df["HT_id"].apply(lambda x: strengths[x]) + h)
    df["Lambda_AS"] = np.exp(c + df["AT_id"].apply(lambda x: strengths[x]) - h)
    # Define probabilities.
    home_goals_probs = np.array(
        [
            np.exp(-df["Lambda_HS"]) * df["Lambda_HS"] ** i / np.math.factorial(i)
            for i in range(max_goals)
        ]
    )
    # Calculate Poisson probabilities for Away team goals.
    away_goals_probs = np.array(
        [
            np.exp(-df["Lambda_AS"]) * df["Lambda_AS"] ** i / np.math.factorial(i)
            for i in range(max_goals)
        ]
    )
    # Initialize probabilities.
    prob_home_win = np.zeros(len(df))
    prob_away_win = np.zeros(len(df))
    prob_draw = np.zeros(len(df))
    # Calculate the probabilities of home win, away win, and draw.
    for i in range(max_goals):
        for j in range(max_goals):
            prob = home_goals_probs[i] * away_goals_probs[j]
            if apply_dixon_coles:
                prob *= dixon_coles_adjustment(
                    i, j, df["Lambda_HS"], df["Lambda_AS"], rho
                )
            prob_home_win += np.where(i > j, prob, 0)
            prob_away_win += np.where(i < j, prob, 0)
            prob_draw += np.where(i == j, prob, 0)
    # Add probabilities to the DataFrame.
    df["prob_home_win"] = prob_home_win
    df["prob_away_win"] = prob_away_win
    df["prob_draw"] = prob_draw
    # Predict the outcomes based on probabilities.
    df["predicted_outcome"] = np.where(
        df["prob_home_win"] > df["prob_away_win"],
        "home_win",
        np.where(df["prob_away_win"] > df["prob_home_win"], "away_win", "draw"),
    )
    # Calculate actual outcomes for comparison.
    df["actual_outcome"] = np.where(
        df["HS"] > df["AS"],
        "home_win",
        np.where(df["HS"] < df["AS"], "away_win", "draw"),
    )
    # Round off the predicted goals to integers.
    df["Lambda_HS"] = df["Lambda_HS"].round().astype(int)
    df["Lambda_AS"] = df["Lambda_AS"].round().astype(int)
    print(df.head())
    return df


# %%
final_df = calculate_match_outcomes(final_data[553:], optimized_params)
# Evaluate model.
rasoprut.evaluate_model_predictions(
        final_df["actual_outcome"], final_df["predicted_outcome"]
    )
