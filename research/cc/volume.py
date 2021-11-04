import pandas as pd

import core.config.config_ as ccocon


def compute_cum_volume(
    data: pd.DataFrame,
    config: ccocon.Config,
    nom_volume: bool,
) -> pd.DataFrame:
    """
    Get cumulative volume on exchange-currency level.

    :param data: crypto-price data
    :param config: config
    :return: cumulative volume per exchange per currency pair
    """
    if nom_volume:
        data["volume"] = volume*price
    data_reset = data.reset_index()
    data_grouped = data_reset.groupby(
        [
            config["column_names"]["exchange"],
            config["column_names"]["currency_pair"],
        ],
        as_index=False
    )
    cum_volume = data_grouped[config["column_names"]["volume"]].sum()
    return cum_volume
