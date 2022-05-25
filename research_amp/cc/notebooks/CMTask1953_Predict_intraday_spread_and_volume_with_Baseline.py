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
# %load_ext autoreload
# %autoreload 2


import logging
import warnings
from datetime import timedelta

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import sklearn.metrics as metrics
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import GridSearchCV, TimeSeriesSplit, cross_val_score

import core.explore as coexplor
import core.features as cofeatur
import helpers.hdbg as hdbg
import helpers.hpandas as hpandas
import helpers.hprint as hprint
import research_amp.transform as ramptran

# %%
warnings.filterwarnings("ignore")

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

hprint.config_notebook()

# %% [markdown]
# # Load the data

# %% [markdown]
# ## OHLCV

# %%
# Read saved 1 month of data.
ohlcv_cc = pd.read_csv("/shared_data/cc_ohlcv.csv", index_col="timestamp")
btc_ohlcv = ohlcv_cc[ohlcv_cc["full_symbol"] == "binance::BTC_USDT"]
btc_ohlcv.index = pd.to_datetime(btc_ohlcv.index)
ohlcv_cols = [
    "open",
    "high",
    "low",
    "close",
    "volume",
    "full_symbol",
]
btc_ohlcv = btc_ohlcv[ohlcv_cols]
btc_ohlcv.head(3)

# %% [markdown]
# ## Bid ask data

# %%
# Read saved 1 month of data.
bid_ask_btc = pd.read_csv(
    "/shared_data/bid_ask_btc_jan22_1min_last.csv", index_col="timestamp"
)
bid_ask_btc.index = pd.to_datetime(bid_ask_btc.index)

# Transform the data.
bid_ask_btc.index = pd.to_datetime(bid_ask_btc.index)
# Compute bid ask stats.
bid_ask_btc = ramptran.calculate_bid_ask_statistics(bid_ask_btc)
# Choose only necessary values.
bid_ask_btc = bid_ask_btc.swaplevel(axis=1)["binance::BTC_USDT"][
    ["bid_size", "ask_size", "bid_price", "ask_price", "mid", "quoted_spread"]
]
bid_ask_btc.index = bid_ask_btc.index.shift(-1, freq="T")

bid_ask_btc.head(3)

# %% [markdown]
# ## Combined

# %%
# OHLCV + bid ask
btc = pd.concat([btc_ohlcv, bid_ask_btc], axis=1)
btc.head(3)


# %% [markdown]
# # Create and test functions for each estimator

# %% [markdown]
# ## Estimate intraday spread, volume

# %%
def get_target_value(df: pd.DataFrame, timestamp: pd.Timestamp, column_name: str):
    """
    :param df: data that contains spread and/or volume
    :param timestamp: timestamp for prediciton
    :param column_name: targeted estimation value (e.g., "quoted_spread", "volume")
    :return: value of targeted spread or volume
    """
    hpandas.dassert_monotonic_index(df.index)
    if timestamp >= df.index.min() and timestamp <= df.index.max():
        value = df[column_name].loc[timestamp]
    else:
        value = np.nan
    return value


# %%
date = pd.Timestamp("2022-01-01 00:01", tz="UTC")
display(get_target_value(btc, date, "quoted_spread"))
display(get_target_value(btc, date, "volume"))


# %% [markdown]
# ## Naive estimator

# %% [markdown]
# Value(t+2) = Value(t)

# %%
def get_naive_value(
    df: pd.DataFrame,
    timestamp: pd.Timestamp,
    column_name: str,
    delay_in_mins: int = 2,
) -> float:
    """
    Estimator for a given time is a `t - delay_in_mins` of a real value.

    :param df: data that contains spread and/or volume
    :param timestamp: timestamp for prediciton
    :param column_name: targeted estimation value (e.g., "quoted_spread", "volume")
    :param delay_in_mins: desired gap for target estimator, in mins
    :return: value of predicted spread or volume
    """
    # Check and define delay.
    hdbg.dassert_lte(1, delay_in_mins)
    delay_in_mins = timedelta(minutes=delay_in_mins)
    # Get the value.
    lookup_timestamp = timestamp - delay_in_mins
    if lookup_timestamp >= df.index.min() and lookup_timestamp <= df.index.max():
        value = get_target_value(df, lookup_timestamp, column_name)
    else:
        value = np.nan
    return value


# %%
date = pd.Timestamp("2022-01-01 00:03", tz="UTC")
display(get_naive_value(btc, date, "quoted_spread"))
display(get_naive_value(btc, date, "volume"))

# %% [markdown]
# ## Look back N days

# %% [markdown]
# spread_lookback(t) = E_date[spread(t, date)]

# %%
# Add column with intraday time.
btc["time"] = btc.index.time


# %%
def get_lookback_value(
    df: pd.DataFrame,
    timestamp: pd.Timestamp,
    lookback_days: int,
    column_name: str,
    delay: int = 0,
    mode: str = "mean",
) -> float:
    """
    1) Set the period that is equal `timestamp for prediciton` - N days (lookback_days).
    2) For that period, calculate mean (or median) value for spread in time during days.
    3) Choose this mean value as an estimation for spread in the given timestamp.

    :param df: data that contains spread
    :param timestamp: timestamp for prediciton
    :param lookback_days: historical period for estimation, in days
    :param column_name: targeted estimation value (e.g., "quoted_spread", "volume")
    :param delay: how many minutes to substract from the lookback starting period
    :param mode: 'mean' or 'median'
    :return: value of predicted spread
    """
    # Choose sample data using lookback period (with a delay).
    start_date = timestamp - timedelta(days=lookback_days, minutes=delay)
    if start_date >= df.index.min() and start_date <= df.index.max():
        sample = df.loc[start_date:timestamp]
        # Look for the reference value for the period.
        time_grouper = sample.groupby("time")
        if mode == "mean":
            grouped = time_grouper[column_name].mean().to_frame()
        else:
            grouped = time_grouper[column_name].median().to_frame()
        # Choose the lookback spread for a given time.
        value = get_target_value(grouped, timestamp.time(), column_name)
    else:
        value = np.nan
    return value


# %%
date = pd.Timestamp("2022-01-21 19:59", tz="UTC")
display(get_lookback_value(btc, date, 14, "quoted_spread"))
display(get_lookback_value(btc, date, 14, "volume"))

# %% [markdown]
# # Collect all estimators for the whole period

# %%
# Hardcoded resampling for `volume`.
btc_volume = btc.resample("5T").volume.sum().rename("volume").to_frame()
btc_volume["time"] = btc_volume.index.time
btc_volume.head(3)


# %%
def collect_real_naive_lookback_est(
    estimators_df: pd.DataFrame,
    original_df,
    target: str,
    est_type: str,
    delay_in_mins: int = 2,
    lookback: int = 14,
) -> pd.DataFrame:
    """
    :param target: e.g., "spread" or "volume"
    :param est_type: e.g., "real", "naive" or "lookback"
    """
    estimators_df[f"{est_type}_{target}"] = estimators_df.index
    # Add the values of a real value.
    if est_type == "real":
        estimators_df[f"{est_type}_{target}_0"] = estimators_df[
            f"{est_type}_{target}"
        ].apply(lambda x: get_target_value(original_df, x, target))
        estimators_df = estimators_df.drop(columns=[f"{est_type}_{target}"])
    # Add the values of naive estimator.
    elif est_type == "naive":
        estimators_df[f"{est_type}_{target}"] = estimators_df[
            f"{est_type}_{target}"
        ].apply(lambda x: get_naive_value(original_df, x, target, delay_in_mins))
    # Add the values of lookback estimator.
    else:
        estimators_df[f"{est_type}_{target}"] = estimators_df[
            f"{est_type}_{target}"
        ].apply(lambda x: get_lookback_value(original_df, x, lookback, target))
    return estimators_df


# %%
# Generate the separate DataFrame for estimators.
estimators = pd.DataFrame(index=btc_volume.index[1:])
# Choose the target value.
target = "volume"

estimators = collect_real_naive_lookback_est(
    estimators, btc_volume, target, "real"
)
estimators = collect_real_naive_lookback_est(
    estimators, btc_volume, target, "naive", 5
)
estimators = collect_real_naive_lookback_est(
    estimators, btc_volume, target, "lookback"
)
estimators


# %% [markdown]
# # Evaluate results

# %%
def get_mean_error(
    df: pd.DataFrame,
    column_name_actual: str,
    column_name_estimator: str,
    num_std: int = 1,
    print_results: bool = True,
) -> pd.Series:
    """
    - Calculate the error of difference between real and estimated values.
    - Show the mean and ± num_std*standard_deviation levels.

    :param df: data with real values and estimators
    :param column_name_actual: e.g., "spread", "volume")
    :param column_name_estimator: estimator (e.g., "naive_spread", "lookback_spread")
    :param num_std: number of standard deviations from mean
    :param print_results: whether or not print results
    :return: errors for each data point
    """
    err = (
        abs(df[column_name_actual] - df[column_name_estimator])
        / df[column_name_actual]
    )
    err_mean = err.mean()
    err_std = err.std()
    if print_results:
        print(
            f"Mean error + {num_std} std = {err_mean+num_std*err_std} \
              \nMean error = {err_mean}\
              \nMean error - {num_std} std = {err_mean-num_std*err_std}"
        )
    return err


# %%
# Choose the period that is equally filled by both estimators.
test = estimators[estimators["lookback_volume"].notna()]
test.head(3)

# %% [markdown]
# ## Naive estimator

# %%
# Mean error and upper/lower level of errors' standard deviation.
column_name_actual = "real_volume_0"
column_name_estimator = "naive_volume"
naive_err = get_mean_error(test, column_name_actual, column_name_estimator)

# %% run_control={"marked": false}
# Regress (OLS) between `real_spread` and `naive_spread`.
predicted_var = "real_volume_0"
predictor_vars = "naive_volume"
intercept = True
# Run OLS.
coexplor.ols_regress(
    test,
    predicted_var,
    predictor_vars,
    intercept,
)

# %%
test[["real_volume_0", "naive_volume"]].plot(figsize=(15, 7))

# %% [markdown]
# ## Lookback estimator

# %%
# Mean error and upper/lower level of errors' standard deviation.
column_name_actual = "real_volume_0"
column_name_estimator = "lookback_volume"
lookback_err = get_mean_error(test, column_name_actual, column_name_estimator)

# %%
# Regress (OLS) between `real_spread` and `lookback_spread`.
predicted_var = "real_volume_0"
predictor_vars = "lookback_volume"
intercept = True
# Run OLS.
coexplor.ols_regress(
    test,
    predicted_var,
    predictor_vars,
    intercept,
)

# %%
test[["real_volume_0", "lookback_volume"]].plot(figsize=(15, 7))


# %% [markdown]
# # Predict via sklearn

# %% [markdown]
# ## Functions

# %%
def regression_results(y_true, y_pred, mae_only: bool = True):
    # Regression metrics
    mean_absolute_error = metrics.mean_absolute_error(y_true, y_pred)
    print("MAE: ", round(mean_absolute_error, 4))
    if not mae_only:
        explained_variance = metrics.explained_variance_score(y_true, y_pred)
        mse = metrics.mean_squared_error(y_true, y_pred)
        mean_squared_log_error = metrics.mean_squared_log_error(y_true, y_pred)
        metrics.median_absolute_error(y_true, y_pred)
        r2 = metrics.r2_score(y_true, y_pred)
        print("mean_squared_log_error: ", round(mean_squared_log_error, 4))
        print("explained_variance: ", round(explained_variance, 4))
        print("r2: ", round(r2, 4))
        print("MAE: ", round(mean_absolute_error, 4))
        print("MSE: ", round(mse, 4))
        print("RMSE: ", round(np.sqrt(mse), 4))


# %% [markdown]
# ## Defining training and test sets

# %%
# Drop NaNs.
test_sk = hpandas.dropna(test)
# Get rid of days with only one observations (first and last rows).
test_sk = test_sk.iloc[1:-1]
# Add more lags as features
test_sk, info = cofeatur.compute_lagged_features(
    test_sk, "real_volume_0", delay_lag=1, num_lags=3
)
# Hardcoded solution: omit "naive" estimator in favor of new lag estimators.
test_sk = test_sk.drop(columns=[f"real_{target}_2"])
print(info)
# Add 2 more features with value difference.
test_sk["naive_real_diff"] = (
    test_sk[f"naive_{target}"] - test_sk[f"real_{target}_0"]
)
test_sk["naive_look_diff"] = (
    test_sk[f"naive_{target}"] - test_sk[f"lookback_{target}"]
)
test_sk["real_look_diff"] = (
    test_sk[f"lookback_{target}"] - test_sk[f"real_{target}_0"]
)
# Display the results.
display(test_sk.corr())
display(test_sk.shape)
display(test_sk.tail(3))
print(f"Set of prediciton features = {list(test_sk.columns[1:])}")

# %%
# Training dataset: first 14 days.
X_train = test_sk.loc["2022-01-15":"2022-01-28"].drop(
    [f"real_{target}_0"], axis=1
)
y_train = test_sk.loc["2022-01-15":"2022-01-28", f"real_{target}_0"]

# Testing dataset: last 3 days.
X_test = test_sk.loc["2022-01-29":"2022-01-31"].drop([f"real_{target}_0"], axis=1)
y_test = test_sk.loc["2022-01-29":"2022-01-31", f"real_{target}_0"]

# %% [markdown]
# The `TimeSerieSplit` function takes as input the number of splits. Since our training data has 14 unique days (2022-01-15 - 2022-01-28), we would be setting `n_splits = 14`.

# %%
n_splits = 14

# %% [markdown]
# ## Models Evaluation

# %%
# Create a set of various estimation modes.
models = []
models.append(("LR", LinearRegression()))
# models.append(("NN", MLPRegressor(solver="lbfgs")))  # neural network
# models.append(("KNN", KNeighborsRegressor()))
# models.append(
#    ("RF", RandomForestRegressor(n_estimators=10))
# )  # Ensemble method - collection of many decision trees
# models.append(("SVR", SVR(gamma="auto")))  # kernel = linear
models

# %%
# Evaluate each model in turn
results = []
names = []
results_stats = pd.DataFrame()
for name, model in models:
    # TimeSeries Cross validation
    tscv = TimeSeriesSplit(n_splits=n_splits)

    cv_results = cross_val_score(model, X_train, y_train, cv=tscv, scoring="r2")
    results.append(cv_results)
    names.append(name)
    print("%s: %f (%f)" % (name, cv_results.mean(), cv_results.std()))

    results_stats.loc[name, "mean_perf"] = cv_results.mean()
    results_stats.loc[name, "std_dev_perf"] = cv_results.std()

display(results_stats.sort_values("mean_perf", ascending=False))

# %%
# Compare Algorithms
plt.boxplot(results, labels=names)
plt.title("Algorithm Comparison")
plt.show()

# %% [markdown]
# ### Grid Searching Hyperparameters (LinearRegression)

# %% run_control={"marked": false}
# Model param variations.
model = LinearRegression()
param_search = {
    "fit_intercept": [True, False],
    "normalize": [True, False],
    "n_jobs": [1, 20, 50],
    "positive": [True, False],
}
tscv = TimeSeriesSplit(n_splits=n_splits)
# If scoring = None, the estimator's score method is used.

# Run the model with different param variations.
gsearch = GridSearchCV(
    estimator=model,
    cv=tscv,
    param_grid=param_search,
)
gsearch.fit(X_train, y_train)

# %%
# Results of the best param fit.
best_score = gsearch.best_score_
best_model = gsearch.best_estimator_
display(best_score)
display(best_model)

# %% [markdown]
# #### Evaluate results using testing sample

# %%
# Estimate testing results.
y_true = y_test.values
y_pred = best_model.predict(X_test)
regression_results(y_true, y_pred)

# %%
# Plot the results of predicting on testing sample.
lr_test = pd.concat([pd.Series(y_true), pd.Series(y_pred)], axis=1)
lr_test.columns = ["true", "predicted"]
lr_test.index = y_test.index
lr_test.plot(figsize=(15, 7))

# %%
# Plot the difference between true and predicted values.
lr_test["diff"] = lr_test["true"] - lr_test["predicted"]
lr_test["diff"].plot(figsize=(15, 7))
