#!/usr/bin/env python

"""
Execute `process_forecasts.py` over a tiled backtest.
"""

import argparse
import asyncio
import datetime
import logging

import core.config as cconfig
import helpers.hasyncio as hasynci
import helpers.hdbg as hdbg
import helpers.hparser as hparser
import oms.process_forecasts_example as oprfoexa
import oms.tiled_process_forecasts as otiprfor

_LOG = logging.getLogger(__name__)

# #############################################################################


# TODO(Paul): Specify
#   - backtest file name
#   - asset_id_col
#   - log_dir
def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    # TODO(Paul): Consider making these optional or else pointing to config
    #   files.
    parser.add_argument(
        "--backtest_file_name",
        action="store",
        help="Path to the parquet tiles containing the forecasts generated by the backtest",
    )
    parser.add_argument(
        "--asset_id_col", action="store", help="Name of the asset id column"
    )
    parser.add_argument(
        "--log_dir", action="store", help="Directory for logging results"
    )
    hparser.add_verbosity_arg(parser)
    return parser


def get_market_data_tile_config() -> cconfig.Config:
    dict_ = {
        "file_name": "/cache/tiled.bar_data.all.2010_2022.20220204",
        "price_col": "close",
        "knowledge_datetime_col": "end_time",
        "start_time_col": "start_time",
        "end_time_col": "end_time",
    }
    config = cconfig.get_config_from_nested_dict(dict_)
    return config


def get_backtest_tile_config() -> cconfig.Config:
    dict_ = {
        "file_name": "",
        "asset_id_col": "",
        "start_date": datetime.date(2020, 12, 1),
        "end_date": datetime.date(2020, 12, 31),
        "prediction_col": "prediction",
        "volatility_col": "vwap.ret_0.vol",
        "spread_col": "pct_bar_spread",
    }
    config = cconfig.get_config_from_nested_dict(dict_)
    return config


async def _run_coro(
    event_loop: asyncio.AbstractEventLoop,
    market_data_tile_config: cconfig.Config,
    backtest_tile_config: cconfig.Config,
    process_forecasts_config: cconfig.Config,
) -> None:
    await otiprfor.run_tiled_process_forecasts(
        event_loop,
        market_data_tile_config,
        backtest_tile_config,
        process_forecasts_config,
    )


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    market_data_tile_config = get_market_data_tile_config()
    _LOG.info("market_data_tile_config=\n%s", str(market_data_tile_config))
    backtest_tile_config = get_backtest_tile_config()
    process_forecasts_config = oprfoexa.get_process_forecasts_config_example1()
    # TODO(Paul): Warn if we are overriding.
    backtest_tile_config["file_name"] = args.backtest_file_name
    backtest_tile_config["asset_id_col"] = args.asset_id_col
    _LOG.info("backtest_tile_config=\n%s", str(backtest_tile_config))
    process_forecasts_config["log_dir"] = args.log_dir
    _LOG.info("process_forecasts_config=\n%s", str(process_forecasts_config))
    with hasynci.solipsism_context() as event_loop:
        hasynci.run(
            _run_coro(
                event_loop,
                market_data_tile_config,
                backtest_tile_config,
                process_forecasts_config,
            ),
            event_loop,
        )


if __name__ == "__main__":
    _main(_parse())
