import logging
from typing import Dict, Iterable

import pandas as pd

import core.explore as coex
import core.signal_processing as sigp
import vendors.kibot.utils as kut

_LOG = logging.getLogger(__name__)


# TODO(Julia): The same is in too many places. Let's consolidate / merge in
#  kibot/utils.py
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


# TODO(Julia): Let's make this code independent on price, like we did for
#  TimeSeriesStudy, and then we can merge this analysis inside
#  TimeSeries so that one can ask questions like "what was the largest movement
#  by day" resampling on the flight.
#  Unfortunately this code needs a data frame so we can't easily put this
#  inside TimeSeriesStudy. Maybe we can have a DataFrameAnalyzer that does this
#  cross-sectional analysis.
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
        # TODO(J): This should not be here, but done by the client.
        rets = compute_kibot_returns(price_df_dict[symbol], period)
        # TODO(J): This should be optional, since the client might have
        #  already done that.
        zscored_ret = sigp.rolling_zscore(rets, tau, demean=False)
        zscored_ret = _choose_movements(zscored_ret, sign)
        # TODO(J): Parametrize with respect to the column.
        zscored_ret = coex.drop_na(pd.DataFrame(zscored_ret), drop_infs=True)[
            "ret_0"
        ]
        zscored_returns.append(zscored_ret)
    zscored_returns = pd.concat(zscored_returns, axis=1)
    mean_zscored_rets = zscored_returns.mean(axis=1, skipna=True)
    ascending = _get_order(sign)
    return mean_zscored_rets.sort_values(ascending=ascending).head(n_movements)


# TODO(J): Not sure the replication of this and the above function is necessary.
#  The general case is a function that gets a dataframe, then one can
#  select a group of securities or a single one to perform the analysis.
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
