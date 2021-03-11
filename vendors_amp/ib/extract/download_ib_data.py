#!/usr/bin/env python

import argparse
import logging

import pandas as pd

import helpers.dbg as dbg
import helpers.parser as prsr
import vendors_amp.ib.extract.utils as ibutils

# from tqdm.notebook import tqdm

_LOG = logging.getLogger(__name__)


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    dbg.shutup_chatty_modules()
    #
    if False:
        target = "forex"
        frequency = "intraday"
        symbols = ["EURUSD"]
    elif False:
        target = "futures"
        frequency = "intraday"
        symbols = ["ES"]
    elif True:
        target = "continuous_futures"
        frequency = "intraday"
        symbols = ["ES"]
    if False:
        df = pd.read_csv("./ib_futures.csv")
        # print(df)
        symbols = df["IB SYMBOL"][:10]
    #symbols = "EUR GBP ES".split()
    symbols = "ES".split()
    tasks = []
    ib = ibutils.ib_connect(0, is_notebook=False)
    use_rth = False
    start_ts = None
    #start_ts = pd.Timestamp("2020-12-13 18:00:00-05:00")
    end_ts = None
    #end_ts = pd.Timestamp("2020-12-23 18:00:00-05:00")
    tasks = ibutils.get_tasks(ib, target, frequency, symbols, start_ts, end_ts,
                              use_rth)
    num_threads = 3
    num_threads = "serial"
    dst_dir = args.dst_dir
    incremental = not args.not_incremental
    client_id_base = 5
    file_names = ibutils.download_ib_data(client_id_base, tasks,
                                          incremental, dst_dir, num_threads)
    _LOG.info("file_names=%s", file_names)


# # ################################################################################
#
# import asyncio
#
# class App:
#
#     async def run(self,
#                   contract, start_ts, end_ts,
#                   bar_size_setting,
#                   what_to_show, use_rth
#                   ):
#
#         self.ib = ib_insync.IB()
#         with await self.ib.connectAsync():
#             duration_str = "2 D"
#             # df = await ibutils.get_historical_data(self.ib,
#             #                                        contract, start_ts, end_ts,
#             #                                        bar_size_setting,
#             #                                        what_to_show, use_rth, use_progress_bar=False,
#             #                                        return_ts_seq=False)
#             df = await ibutils.req_historical_data(self.ib, contract, end_ts,
#                                                    duration_str,
#                                                    bar_size_setting,
#                                                    what_to_show, use_rth)
#             print(df)
#             return df
#
#     def stop(self):
#         self.ib.disconnect()
#
#
# app = App()
#
# async def download():
#     ib = ibutils.ib_connect(0, is_notebook=False)
#     target = "continuous_futures"
#     frequency = "intraday"
#     symbols = ["ES"]
#     start_ts = pd.Timestamp("2018-02-07 17:59").tz_localize(tz="America/New_York")
#     end_ts = start_ts + pd.DateOffset(days=3)
#     symbol = symbols[0]
#     contract, duration_str, bar_size_setting, what_to_show = _select_assets(ib, target, frequency, symbol)
#     use_rth = True
#     duration_str = "2 D"
#     df = await ibutils.req_historical_data(ib, contract, end_ts,
#                             bar_size_setting,
#                             what_to_show, use_rth, use_progress_bar=False,
#                             return_ts_seq=False)
#     return df
#
#
# def _main3(parser: argparse.ArgumentParser) -> None:
#     args = parser.parse_args()
#     dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
#     dbg.shutup_chatty_modules()
#     #
#     # if False:
#     #     target = "forex"
#     #     frequency = "intraday"
#     #     symbols = ["EURUSD"]
#     # elif False:
#     #     target = "futures"
#     #     frequency = "intraday"
#     #     symbols = ["ES"]
#     # elif True:
#     #     target = "continuous_futures"
#     #     frequency = "intraday"
#     #     symbols = ["ES"]
#     # #
#     # tasks = []
#     # ib = ibutils.ib_connect(0, is_notebook=False)
#     # for symbol in symbols:
#     #     tasks.append(_select_assets(ib, target, frequency, symbol))
#     # num_threads = args.num_threads
#     # num_threads = 3
#     num_threads = "serial"
#     dst_dir = args.dst_dir
#     incremental = not args.not_incremental
#     # if num_threads == "serial":
#     #     for client_id, task in enumerate(tasks):
#     #         contract, duration_str, bar_size_setting, what_to_show = task
#     #         _process_workload(client_id + 1, contract, duration_str, bar_size_setting,
#     #                           what_to_show, dst_dir, incremental)
#     # else:
#     ib = ibutils.ib_connect(0, is_notebook=False)
#     target = "continuous_futures"
#     frequency = "intraday"
#     symbols = ["ES"]
#     start_ts = pd.Timestamp("2018-02-07 17:59").tz_localize(tz="America/New_York")
#     end_ts = start_ts + pd.DateOffset(days=3)
#     symbol = symbols[0]
#     contract, duration_str, bar_size_setting, what_to_show = _select_assets(ib, target, frequency, symbol)
#     use_rth = True
#     ib.disconnect()
#
#     try:
#         asyncio.run(app.run(
#             contract, start_ts, end_ts,
#             bar_size_setting,
#             what_to_show, use_rth))
#     except (KeyboardInterrupt, SystemExit):
#         app.stop()
#
#
# # #################################################################################


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("positional", nargs="*", help="...")
    parser.add_argument("--dst_dir", action="store", default="./tmp.download_data",
                        help="Destination dir")
    parser.add_argument("--not_incremental", action="store_true", default=False)
    prsr.add_verbosity_arg(parser)
    return parser


if __name__ == "__main__":
    user_start_ts = pd.Timestamp("2018-02-01")
    user_end_ts = pd.Timestamp("2019-06-01")
    _main(_parse())
