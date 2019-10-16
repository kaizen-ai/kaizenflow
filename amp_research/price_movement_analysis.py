import logging
from typing import Dict, Iterable

import pandas as pd

import core.signal_processing as sigp
import helpers.dbg as dbg
import vendors.kibot.utils as kut

_LOG = logging.getLogger(__name__)


def get_zscored_returns(
    prices: pd.DataFrame, period: str, tau: int, demean: bool = False
):
    dbg.dassert_in(period, ["daily", "minutely"])
    # Compute returns.
    if period == "minutely":
        rets = kut.compute_ret_0_from_1min_prices(prices, "log_rets")
    else:
        rets = kut.compute_ret_0_from_daily_prices(prices, "open", "log_rets")
    # z-score.
    zscored_rets = sigp.rolling_zscore(rets, tau, demean=demean)
    _LOG.debug("zscored_rets=\n%s", zscored_rets.head())
    return zscored_rets


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
        zscored_ret = get_zscored_returns(price_df_dict[symbol], period, tau)
        zscored_ret = _choose_movements(zscored_ret, sign)
        zscored_returns.append(zscored_ret)
    zscored_returns = pd.concat(zscored_returns, axis=1)
    mean_zscored_rets = zscored_returns.mean(axis=1, skipna=True)
    return mean_zscored_rets.sort_values(ascending=False).head(n_movements)


def get_top_movements_for_symbol(
    price_df_dict: Dict[str, pd.DataFrame],
    symbol: str,
    period: str,
    tau: int,
    sign: str,
    n_movements: int = 100,
):
    zscored_rets = get_zscored_returns(price_df_dict[symbol], period, tau)
    zscored_rets = _choose_movements(zscored_rets, sign)
    return zscored_rets.sort_values(ascending=False).head(n_movements)


def _choose_movements(zscored_rets, sign):
    dbg.dassert_in(sign, ["pos", "neg", "all"])
    if sign == "pos":
        zscored_rets = zscored_rets.loc[zscored_rets >= 0]
    elif sign == "neg":
        zscored_rets = zscored_rets.loc[zscored_rets < 0]
    else:
        zscored_rets = zscored_rets.abs()
    return zscored_rets
