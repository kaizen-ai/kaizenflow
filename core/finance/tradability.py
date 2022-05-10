import datetime
from typing import List, Optional

import pandas as pd
import s3fs
import pyarrow.parquet as parquet
from tqdm.autonotebook import tqdm

import helpers.hdbg as hdbg


def process_df(df: pd.DataFrame, freq_mins: int) -> pd.DataFrame:
    rics = df["ric"].unique()
    hdbg.dassert_eq(len(rics), 1, "Found more than one ric: %s", rics)
    ric = rics[0]
    #
    egids = df["egid"].unique()
    hdbg.dassert_eq(len(egids), 1, "Found more than one egid: %s", egids)
    egid = egids[0]
    #
    out_df = pd.DataFrame()
    out_df["close"] = df["close"].resample(f"{freq_mins}T").last()
    out_df["ric"] = ric
    out_df["egid"] = egid
    out_df["volume"] = df["volume"].resample(f"{freq_mins}T").sum()
    # # TODO(gp): This should be VWAP.
    out_df["bid"] = df["bid"].resample(f"{freq_mins}T").mean()
    out_df["ask"] = df["ask"].resample(f"{freq_mins}T").mean()
    out_df = out_df.dropna()
    # #
    out_df["ret_0"] = out_df["close"].pct_change()
    out_df["spread_usd"] = (out_df["ask"] - out_df["bid"])
    out_df["spread_bps"] = (out_df["ask"] - out_df["bid"]) / out_df["close"]
    out_df["trad"] = out_df["ret_0"].abs() / out_df["spread_bps"]
    out_df = out_df.dropna()
    return out_df


def compute_stats(df: pd.DataFrame) -> pd.Series:
    rics = df["ric"].unique()
    hdbg.dassert_eq(len(rics), 1, "Found more than 1 future: %s", rics)
    ric = rics[0]
    srs = {
        "ric": ric,
        "close.mean": df["close"].mean(),
        "ret_0.mad": df["ret_0"].mad(),
        "spread_usd.median": df["spread_usd"].median(),
        "spread_bps.median": df["spread_bps"].median(),
        "trad": df["trad"].median(),
    }
    srs = pd.Series(srs)
    return srs


# #################################################################################


import random
import numpy as np

def get_predictions(df: pd.DataFrame, ret_col: str, hit_rate: float, seed: int) -> pd.Series:
    """
    :param df: Desired sample with OHLCV data and calculated returns
    :param hit_rate: Desired percantage of successful predictions
    :param seed: Experiment stance
    """
    hdbg.dassert_lte(0, hit_rate)
    hdbg.dassert_lte(hit_rate, 1)
    random.seed(seed)
    n = df.shape[0]
    rets = df[ret_col].values
    # Mask contains 1 for a desired hit and -1 for a miss.
    num_hits = int((1 - hit_rate) * n)
    mask = ([-1] * num_hits) + ([1] * (n - num_hits))
    # print(len(mask))
    mask = np.asarray(mask)
    # Randomize the location of the outcomes.
    random.shuffle(mask)
    # Construct predictions using the sign of the actual returns.
    pred = pd.Series(np.sign(rets) * mask)
    # Change the index for easy attachment to initial DataFrame.
    pred.index = df.index
    return pred


# TODO(gp): Move to hpandas.
def plot_without_gaps(df):
    #df.plot(x=df.index.astype(str))
    df = df.copy()
    df.index = df.index.to_series().dt.strftime('%Y-%m-%d')
    df.plot()
