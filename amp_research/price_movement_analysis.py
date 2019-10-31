import logging
from typing import Dict, Iterable

import pandas as pd

import core.explore as coex
import core.signal_processing as sigp
import vendors.kibot.utils as kut

_LOG = logging.getLogger(__name__)


def compute_kibot_returns(prices: pd.DataFrame, period: str):
    if period == "minutely":
        rets = kut.compute_ret_0_from_1min_prices(prices, "log_rets")
    elif period == "daily":
        rets = kut.compute_ret_0_from_daily_prices(prices, "close", "log_rets")
    else:
        raise ValueError(
            "Only daily and minutely periods are supported, passed %s" % period
        )
    return rets


def get_top_movements_by_group(
    price_df_dict: Dict[str, pd.DataFrame],
    commodity_symbols_kibot: Dict[str, Iterable],
    group: str,
    period: str,
    tau: int,
    sign: str,
    n_movements: int = 100,
):
    zscored_returns = []
    for symbol in commodity_symbols_kibot[group]:
        rets = compute_kibot_returns(price_df_dict[symbol], period)
        zscored_ret = sigp.rolling_zscore(rets, tau, demean=False)
        zscored_ret = _choose_movements(zscored_ret, sign)
        zscored_ret = coex.drop_na(pd.DataFrame(zscored_ret), drop_infs=True)[
            "ret_0"
        ]
        zscored_returns.append(zscored_ret)
    zscored_returns = pd.concat(zscored_returns, axis=1)
    mean_zscored_rets = zscored_returns.mean(axis=1, skipna=True)
    ascending = _get_order(sign)
    return mean_zscored_rets.sort_values(ascending=ascending).head(n_movements)


def get_top_movements_for_symbol(
    price_df_dict: Dict[str, pd.DataFrame],
    symbol: str,
    period: str,
    tau: int,
    sign: str,
    n_movements: int = 100,
):
    rets = compute_kibot_returns(price_df_dict[symbol], period)
    zscored_rets = sigp.rolling_zscore(rets, tau, demean=False)
    zscored_rets = _choose_movements(zscored_rets, sign)
    ascending = _get_order(sign)
    zscored_rets = coex.drop_na(pd.DataFrame(zscored_rets), drop_infs=True)[
        "ret_0"
    ]
    return zscored_rets.sort_values(ascending=ascending).head(n_movements)


def _choose_movements(zscored_rets, sign):
    if sign == "pos":
        zscored_rets = zscored_rets.loc[zscored_rets >= 0]
    elif sign == "neg":
        zscored_rets = zscored_rets.loc[zscored_rets < 0]
    elif sign == "all":
        zscored_rets = zscored_rets.abs()
    else:
        raise ValueError(
            "Only pos, neg, all signs are supported, passed %s" % sign
        )
    return zscored_rets


def _get_order(sign: str):
    if sign == "neg":
        ascending = True
    elif sign in ["pos", "all"]:
        ascending = False
    else:
        raise ValueError(
            "Only pos, neg, all signs are supported, passed %s" % sign
        )
    return ascending
