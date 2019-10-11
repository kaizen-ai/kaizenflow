import pandas as pd

import core.signal_processing as sp

TAU = 2


def get_zscored_prices_diff(price_dict_df, symbol, tau=TAU):
    prices_symbol = price_dict_df[symbol]
    prices_diff = prices_symbol["close"] - prices_symbol["open"]
    zscored_prices_diff = sp.rolling_zscore(prices_diff, tau)
    zscored_prices_diff.head()
    abs_zscored_prices_diff = zscored_prices_diff.abs()
    return abs_zscored_prices_diff


def get_top_movements_by_group(
    price_dict_df, commodity_symbols_kibot, group, n_movements=100
):
    zscored_diffs = []
    for symbol in commodity_symbols_kibot[group]:
        zscored_diff = get_zscored_prices_diff(price_dict_df, symbol)
        zscored_diffs.append(zscored_diff)
    zscored_diffs = pd.concat(zscored_diffs, axis=1)
    mean_zscored_diffs = zscored_diffs.mean(axis=1, skipna=True)
    return mean_zscored_diffs.sort_values(ascending=False).head(n_movements)


def get_top_movements_for_symbol(price_dict_df, symbol, tau=TAU, n_movements=100):
    zscored_diffs = get_zscored_prices_diff(price_dict_df, symbol, tau=tau)
    return zscored_diffs.sort_values(ascending=False).head(n_movements)
