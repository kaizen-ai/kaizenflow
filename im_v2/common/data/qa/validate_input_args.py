#!/usr/bin/env python
"""
Single dataset OHLCV QA checks.

Import as:

import im_v2.common.data.qa.validate_input_args as imvcdqvia
"""
import argparse

import helpers.hdbg as hdbg
import im_v2.binance.data.extract.extractor as imvbdexex
import im_v2.ccxt.data.extract.extractor as imvcdexex
import im_v2.common.data.extract.extractor as ivcdexex
import im_v2.crypto_chassis.data.extract.extractor as imvccdexex


def validate_vendor_arg(
    vendor: str, args: argparse.Namespace
) -> ivcdexex.Extractor:
    """
    Function to centralizing checking logical dependencies between values of
    various arguments for ETL scripts.

    :param vendor: string to get the vendor
    :param args: args to get the contract type
    """
    if vendor == "crypto_chassis":
        if not args.get("universe_part"):
            raise RuntimeError(
                f"--universe_part argument is mandatory for {vendor}"
            )
        exchange = imvccdexex.CryptoChassisExtractor(args["contract_type"])
    elif vendor == "ccxt":
        exchange = imvcdexex.CcxtExtractor(
            args["exchange_id"], args["contract_type"]
        )
    elif vendor == "binance":
        # For the bulk download, we allow data gaps.
        exchange = imvbdexex.BinanceExtractor(
            args["contract_type"],
            allow_data_gaps=True,
            # TODO(Vlad): Temporary stick to daily data for Binance.
            time_period=imvbdexex.BinanceNativeTimePeriod.DAILY,
        )
    else:
        hdbg.dfatal(f"Vendor {vendor} is not supported.")
    return exchange
