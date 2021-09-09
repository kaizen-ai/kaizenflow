import logging

import numpy as np
import pandas as pd

import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)

_LOG.debug = _LOG.info


def compute_data(num_samples: int, seed: int = 42) -> pd.DataFrame:
    np.random.seed(seed)
    date_range = pd.date_range("09:30", periods=num_samples, freq="1T")
    # Random walk.
    diff = np.random.normal(0, 1, size=len(date_range))
    diff = diff.cumsum()
    price = 100.0 + diff
    #
    df = pd.DataFrame(price, index=date_range, columns=["price"])
    # Add ask, bid.
    df["ask"] = price + np.abs(np.random.normal(0, 1, size=len(date_range)))
    df["bid"] = price - np.abs(np.random.normal(0, 1, size=len(date_range)))
    return df
    #display(df.head(5))


def resample_data(df: pd.DataFrame, mode: str, seed: int = 42) -> pd.DataFrame:
    # Sample on 5 minute bars labeling and close on the right
    df_5mins = df.resample("5T", closed="right", label="right")
    if mode == "instantaneous":
        df_5mins = df_5mins.last()
    elif mode == "twap":
        df_5mins = df_5mins.mean()
    else:
        raise ValueError("Invalid mode='%s'" % mode)
    # Compute ret_0.
    df_5mins["ret_0"] = df_5mins["price"].pct_change()
    # Compute predictions.
    np.random.seed(seed)
    vals = (np.random.random(df_5mins.shape[0]) >= 0.5) * 2.0 - 1.0
    # nan the last two predictions since we need to rows to enter / exit.
    #vals[-2:] = np.nan
    vals[-2:] = 0
    df_5mins["preds"] = vals
    return df_5mins


def compute_pnl_for_instantaneous_no_cost_case(w0: float, df: pd.DataFrame,
                                               df_5mins: pd.DataFrame):
    w = w0
    # Skip the last two rows since we need two rows to enter / exit the position.
    for ts, row in df_5mins[:-2].iterrows():
        _LOG.debug("ts=%s", ts)
        pred = row["preds"]
        #
        ts_5 = ts + pd.DateOffset(minutes=5)
        dbg.dassert_in(ts_5, df.index)
        price_5 = df.loc[ts_5]["price"]
        #
        ts_10 = ts + pd.DateOffset(minutes=10)
        dbg.dassert_in(ts_10, df.index)
        price_10 = df.loc[ts_10]["price"]
        _LOG.debug("# pred=%s price_5=%s price_10=%s", pred, price_5, price_10)
        #
        num_shares = w / price_5
        if pred == 1:
            # Go long.
            buy_pnl = num_shares * price_5
            sell_pnl = num_shares * price_10
            diff = -buy_pnl + sell_pnl
        elif pred == -1:
            # Short sell.
            sell_pnl = num_shares * price_5
            buy_pnl = num_shares * price_10
            diff = sell_pnl - buy_pnl
        else:
            raise ValueError
        _LOG.debug("  w=%s num_shares=%s", w, num_shares)
        w += diff
        _LOG.debug("  diff=%s -> w=%s", diff, w)
    total_ret = (w - w0) / w0
    return w, total_ret