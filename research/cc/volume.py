# %%
import pandas as pd

# %%
import core.config.config_ as ccocon
import core.plotting as cplot


# %%
def compute_cum_volume(
    data: pd.DataFrame,
    config: ccocon.Config,
    nom_volume: bool,
) -> pd.DataFrame:
    """
    Get cumulative volume on exchange-currency level.

    :param data: crypto-price data
    :param config: config
    :param nom_value: volume is nominated in US Dollars
    :return: cumulative volume per exchange per currency pair
    """
    if nom_volume:
        data["volume"] = data["volume"]*data["close"]
    data_reset = data.reset_index()
    data_grouped = data_reset.groupby(
        [
            config["column_names"]["exchange"],
            config["column_names"]["currency_pair"],
        ],
        as_index=False
    )
    cum_volume = data_grouped[config["column_names"]["volume"]].sum()
    cum_volume["avg_daily_volume"] = cum_volume[config["column_names"]["volume"]]/data_reset.iloc[:,6].shape[0]
    
    return cum_volume

def get_total_volume_by_coins(
    data: pd.DataFrame,
    config: ccocon.Config,
    avg_daily: bool,
): # -> printed Series, plot
    """
    Print total volume by coins values and plot them on barchart 
    
    :param data: cumulative volume per exchange per currency pair
    :param config: config
    :param avg_daily: volume is normalised by days
    """
    if avg_daily:
        coin_volume = data.groupby("currency_pair")[config["column_names"]["avg_daily_volume"]].sum()
    else:
        coin_volume = data.groupby("currency_pair")[config["column_names"]["volume"]].sum()
    coin_volume = coin_volume.sort_values(ascending=False)
    print(coin_volume)
    cplot.plot_barplot(coin_volume,title="Total volume per coin",figsize=[15,7],yscale="log")
    
def get_total_volume_by_exchange(
    data: pd.DataFrame,
    config: ccocon.Config,
    avg_daily: bool,
): # -> printed Series, plot
    """
    Print total volume by exchange values and plot them on barchart 
    
    :param data: cumulative volume per exchange per currency pair
    :param config: config
    :param avg_daily: volume is normalised by days
    """
    if avg_daily:
        exchange_volume = data.groupby("exchange_id")[config["column_names"]["avg_daily_volume"]].sum()
    else:
        exchange_volume = data.groupby("exchange_id")[config["column_names"]["volume"]].sum()
    exchange_volume = exchange_volume.sort_values(ascending=False)
    print(exchange_volume)
    cplot.plot_barplot(exchange_volume,title="Total volume per exchange",figsize=[15,7],yscale="log")
