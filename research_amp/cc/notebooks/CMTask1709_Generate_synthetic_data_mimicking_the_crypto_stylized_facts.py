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
# - Compute returns from the real data.
# - Pre-define the hit rate and calculate predictions, hits and confidence intervals.

# %% [markdown]
# # Imports

# %%
import logging
from typing import List

import matplotlib.pyplot as plt
import pandas as pd
import requests
import statsmodels

import core.finance.tradability as cfintrad
import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hpandas as hpandas
import helpers.hprint as hprint
import im_v2.common.universe as ivcu

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

hprint.config_notebook()


# %% [markdown]
# # Extract returns from the real data

# %% [markdown]
# ## Load BTC data from CC

# %%
def get_exchange_currency_for_api_request(full_symbol: str) -> str:
    """
    Return `exchange_id` and `currency_pair` in a format for requests to cc
    API.
    """
    cc_exchange_id, cc_currency_pair = ivcu.parse_full_symbol(full_symbol)
    cc_currency_pair = cc_currency_pair.lower().replace("_", "-")
    return cc_exchange_id, cc_currency_pair


def load_crypto_chassis_ohlcv_for_one_symbol(full_symbol: str) -> pd.DataFrame:
    """
    - Transform CK `full_symbol` to the `crypto-chassis` request format.
    - Construct OHLCV data request for `crypto-chassis` API.
    - Save the data as a DataFrame.
    """
    # Deconstruct `full_symbol`.
    cc_exchange_id, cc_currency_pair = get_exchange_currency_for_api_request(
        full_symbol
    )
    # Build a request.
    r = requests.get(
        f"https://api.cryptochassis.com/v1/ohlc/{cc_exchange_id}/{cc_currency_pair}?startTime=0"
    )
    # Get url with data.
    url = r.json()["historical"]["urls"][0]["url"]
    # Read the data.
    df = pd.read_csv(url, compression="gzip")
    return df


def apply_ohlcv_transformation(
    df: pd.DataFrame,
    full_symbol: str,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> pd.DataFrame:
    """
    Following transformations are applied:

    - Convert `timestamps` to the usual format.
    - Convert data columns to `float`.
    - Add `full_symbol` column.
    """
    # Convert `timestamps` to the usual format.
    df = df.rename(columns={"time_seconds": "timestamp"})
    df["timestamp"] = df["timestamp"].apply(
        lambda x: hdateti.convert_unix_epoch_to_timestamp(x, unit="s")
    )
    df = df.set_index("timestamp")
    # Convert to `float`.
    for cols in df.columns:
        df[cols] = df[cols].astype(float)
    # Add `full_symbol`.
    df["full_symbol"] = full_symbol
    # Note: I failed to put [start_time, end_time] to historical request.
    # Now it loads all the available data.
    # For that reason the time interval is hardcoded on this stage.
    df = df.loc[(df.index >= start_date) & (df.index <= end_date)]
    return df


def read_crypto_chassis_ohlcv(
    full_symbols: List[str], start_date: pd.Timestamp, end_date: pd.Timestamp
) -> pd.DataFrame:
    """
    - Load the raw data for one symbol.
    - Convert it to CK format.
    - Repeat the first two steps for all `full_symbols`.
    - Concentrate them into unique DataFrame.
    """
    result = []
    for full_symbol in full_symbols:
        # Load raw data.
        df_raw = load_crypto_chassis_ohlcv_for_one_symbol(full_symbol)
        # Process it to CK format.
        df = apply_ohlcv_transformation(df_raw, full_symbol, start_date, end_date)
        result.append(df)
    final_df = pd.concat(result)
    return final_df


# %%
# Commented in order to lad the fast locally.
# btc_df = read_crypto_chassis_ohlcv(
#    ["binance::BTC_USDT"],
#    pd.Timestamp("2021-01-01", tz="UTC"),
#    pd.Timestamp("2022-01-01", tz="UTC"),
# )

# %%
btc_df = pd.read_csv("BTC_one_year.csv", index_col="timestamp")
btc_df.head(3)

# %% [markdown]
# ## Process returns

# %%
btc = btc_df.copy()

# %% run_control={"marked": false}
# Calculate returns.
btc["rets"] = btc["close"].pct_change()
# Rolling SMA for returns (with 100 periods loockback period).
btc["rets_sma"] = btc["rets"].transform(lambda x: x.rolling(window=100).mean())
# Substract SMA from returns to remove the upward trend.
btc["rets_cleaned"] = btc["rets"] - btc["rets_sma"]
# btc["rets"].plot()
btc["rets_cleaned"].plot()

# %%
# Exclude NaNs for the better analysis (at least one in the beginning because of `pct_change()`)
rets = btc[["rets"]]
rets = hpandas.dropna(rets, report_stats=True)

# %% run_control={"marked": false}
# Show the distribution.
fig = plt.figure(figsize=(15, 7))
ax1 = fig.add_subplot(1, 1, 1)
rets.hist(bins=300, ax=ax1)
ax1.set_xlabel("Return")
ax1.set_ylabel("Sample")
ax1.set_title("Returns distribution")
plt.show()


# %% [markdown]
# # Pre-defined Predictions, Hit Rates and Confidence Interval

# %% run_control={"marked": false}
def calculate_confidence_interval(hit_series, alpha, method):
    """
    :param alpha: Significance level
    :param method: "normal", "agresti_coull", "beta", "wilson", "binom_test"
    """
    point_estimate = hit_series.mean()
    hit_lower, hit_upper = statsmodels.stats.proportion.proportion_confint(
        count=hit_series.sum(),
        nobs=hit_series.count(),
        alpha=alpha,
        method=method,
    )
    result_values_pct = [100 * point_estimate, 100 * hit_lower, 100 * hit_upper]
    conf_alpha = (1 - alpha / 2) * 100
    print(f"hit_rate: {result_values_pct[0]}")
    print(f"hit_rate_lower_CI_({conf_alpha}%): {result_values_pct[1]}")
    print(f"hit_rate_upper_CI_({conf_alpha}%): {result_values_pct[2]}")


def get_predictions_hits_and_stats(df, ret_col, hit_rate, seed, alpha, method):
    """
    Calculate hits from the predictions and show confidence intervals.

    :param df: Desired sample with OHLCV data and calculated returns
    :param hit_rate: Desired percantage of successful predictions
    :param seed: Experiment stance
    :param alpha: Significance level for CI
    :param method: "normal", "agresti_coull", "beta", "wilson", "binom_test"
    """
    df = df.copy()
    df["predictions"] = cfintrad.get_predictions(df, ret_col, hit_rate, seed)
    # Specify necessary columns.
    df = df[["rets", "predictions"]]
    # Attach `hit` column (boolean).
    df = df.copy()
    df["hit"] = df["rets"] * df["predictions"] >= 0
    # Exclude NaNs for the better analysis (at least one in the beginning because of `pct_change()`)
    df = hpandas.dropna(df, report_stats=True)
    calculate_confidence_interval(df["hit"], alpha, method)
    return df


# %%
sample = btc.head(1000)
ret_col = "rets"
hit_rate = 0.55
seed = 20
alpha = 0.05
method = "normal"

hit_df = get_predictions_hits_and_stats(
    sample, ret_col, hit_rate, seed, alpha, method
)
display(hit_df)
