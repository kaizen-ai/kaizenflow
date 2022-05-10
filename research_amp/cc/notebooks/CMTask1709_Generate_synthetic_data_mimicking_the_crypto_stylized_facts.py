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
import numpy as np
import pandas as pd
import requests
import seaborn as sns

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

# %%
sample = btc.head(1000)
ret_col = "rets"
hit_rate = 0.51
seed = 2
alpha = 0.05
method = "normal"

hit_df = cfintrad.get_predictions_and_hits(sample, ret_col, hit_rate, seed)
display(hit_df.head(3))
cfintrad.calculate_confidence_interval(hit_df["hit"], alpha, method)

# %% [markdown]
# # PnL as a function of `hit_rate`

# %% [markdown]
# ## Show PnL for the current `hit_rate`

# %%
# A model makes a prediction on the rets and then it's realized.
pnl = (hit_df["predictions"] * hit_df["rets"]).cumsum()
pnl.plot()

# %% [markdown]
# ## Relationship between hit rate and pnl (bootstrapping to compute pnl = f(hit_rate))

# %%
sample = btc.head(1000)
rets_col = "rets"
hit_rates = np.linspace(0.4, 0.6, num=10)
n_experiment = 10

pnls = cfintrad.simulate_pnls_for_set_of_hit_rates(
    hit_df, "rets", hit_rates, n_experiment
)

# %%
hit_pnl_df = pd.DataFrame(pnls.items(), columns=["hit_rate", "PnL"])
sns.scatterplot(data=hit_pnl_df, x="hit_rate", y="PnL")
