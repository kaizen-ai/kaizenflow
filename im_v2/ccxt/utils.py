"""
Import as:

import im_v2.ccxt.utils as imv2ccuti
"""

import helpers.hdbg as hdbg


def convert_currency_pair_to_ccxt_format(
    currency_pair: str,
    exchange: str,
    contract_type: str,
) -> str:
    """
    Convert currency pair used for getting data from exchange.

    :param currency_pair: currency pair, e.g. "BTC_USDT"
    :param exchange_id: exchange from where the data is fetched
    :param contract_type: type of contract e.g. "spot" or "futures"
    """
    hdbg.dassert_in(
        "_",
        currency_pair,
        f"The currency_pair '{currency_pair}' does not match the format '{{str1}}_{{str2}}'.",
    )
    # Unlike other exchanges, Kraken only support spot and has different exchange
    # for futures Krakenfutures.
    if exchange == "kraken":
        hdbg.dassert_eq(contract_type, "spot")
    if exchange == "krakenfutures":
        hdbg.dassert_eq(contract_type, "futures")
    if contract_type == "futures" or contract_type == "swap":
        # In the newer CCXT version symbols are specified using the following format:
        # BTC/USDT:USDT where the :USDT refers to the currency in which the futures contract
        # are settled.
        settlement_coin = currency_pair.split("_")[1]
        currency_pair = currency_pair.replace("_", "/")
        converted_pair = f"{currency_pair}:{settlement_coin}"
    else:
        # E.g., USD_BTC -> USD/BTC.
        converted_pair = currency_pair.replace("_", "/")
    return converted_pair
