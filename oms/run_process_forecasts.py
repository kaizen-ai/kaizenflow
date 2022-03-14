#!/usr/bin/env python

"""
Execute `process_forecasts.py` over a tiled backtest.
"""

import argparse
import logging

import pandas as pd

import helpers.hasyncio as hasynci
import helpers.hdbg as hdbg
import helpers.hparser as hparser

# import oms.portfolio as omportfo
import oms.portfolio_example as oporexam
import oms.process_forecasts as oprofore

# import helpers.hsystem as hsystem

_LOG = logging.getLogger(__name__)

# #############################################################################


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("positional", nargs="*", help="...")
    parser.add_argument("--dst_dir", action="store", help="Destination dir")
    hparser.add_verbosity_arg(parser)
    return parser


async def _run_coro(event_loop):
    #
    # TODO(Paul): Read from tiled backtest.
    # TODO(Paul):
    prediction_df = pd.DataFrame()
    volatility_df = pd.DataFrame()
    config = {}
    # (
    #     market_data,
    #     _,
    # ) = mdata.get_ReplayedTimeMarketData_example3(event_loop)
    portfolio = oporexam.get_DataFramePortfolio_example1(event_loop)
    await oprofore.process_forecasts(
        prediction_df, volatility_df, portfolio, config
    )


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    with hasynci.solipsism_context() as event_loop:
        hasynci.run(_run_coro(event_loop), event_loop)


if __name__ == "__main__":
    _main(_parse())