#!/usr/bin/env python

import argparse
import datetime
import logging

import ib_insync
import pandas as pd
import joblib

import helpers.dbg as dbg
import helpers.io_ as hio
import helpers.parser as hparse
import vendors_amp.ib.extract.download_data_ib_loop as viedow
import vendors_amp.ib.extract.save_historical_data_with_IB_loop as viesav
import vendors_amp.ib.extract.unrolling_download_data_ib_loop as vieunr
import vendors_amp.ib.extract.utils as vieuti

_LOG = logging.getLogger(__name__)

SETUP = dict(
    client_id=102,
    contract=ib_insync.ContFuture(symbol="ES", exchange="GLOBEX", currency="USD"),
    start_ts=pd.Timestamp(year=2020, month=8, day=1),
    end_ts=pd.Timestamp.now(),
    duration_str="1 D",
    bar_size_setting="1 min",
    what_to_show="TRADES",
    use_rth=False,
)


def _run_req_wrapper():
    # Just run direct IB request.
    args = SETUP.copy()
    ib = vieuti.ib_connect(args.pop("client_id"), False)
    contract = ib.qualifyContracts(args.pop("contract"))[0]
    ib.disconnect()
    args.pop("start_ts")
    args.update(dict(
        ib=ib,
        contract=contract,
    ))
    for dur, times in [("10 D", 1), ("1 M", 1) ,("10 D", 3)]:
        _LOG.info("Running request wrapper code %s %s times...", dur, times)
        timer_start = datetime.datetime.now()
        args.update(dict(duration_str=dur))
        for i in range(times):
            ib = vieuti.ib_connect(200+i, False)
            args["ib"] = ib
            df = vieuti.req_historical_data(**args)
            ib.disconnect()
        timer_stop = datetime.datetime.now()
        _LOG.info(
            "Finished. Time: %s seconds. Dataframe: %s",
            (timer_stop - timer_start).total_seconds(),
            vieuti.get_df_signature(df),
        )


def _run_unrolling_download_data_ib_loop():
    # Run latest code.
    _LOG.info("Running unrolling_download_data_ib_loop code...")
    for mode, num_threads, dst_dir in [
        ("in_memory", "serial", "./in_memory"),
        ("on_disk", "serial", "./on_disk_serial"),
        ("on_disk", 4, "./on_disk_4_cores"),
    ]:
        hio.create_dir(dst_dir, incremental=True)
        args = SETUP.copy()
        args.update(dict(
            mode=mode,
            num_threads=num_threads,
            incremental=False,
            dst_dir=dst_dir,
        ))
        args.pop("duration_str")
        _LOG.info(
            "Starting data extraction. Mode: %s. Threads: %s.", mode, num_threads
        )
        timer_start = datetime.datetime.now()
        df = vieunr.get_historical_data(**args)
        timer_stop = datetime.datetime.now()
        _LOG.info(
            "Finished. Time: %s seconds. Dataframe: %s",
            (timer_stop - timer_start).total_seconds(),
            vieuti.get_df_signature(df),
        )
        hio.delete_dir(dst_dir)


def _run_save_historical_data_with_IB_loop():
    # Run saving to separate files.
    _LOG.info("Running save_historical_data_with_IB_loop code...")
    file_name = "./save_ib_loop/experiment.csv"
    hio.create_enclosing_dir(file_name, incremental=True)
    _LOG.info("Starting data extraction.")
    args = SETUP.copy()
    args.update(dict(
        file_name=file_name,
        ib=vieuti.ib_connect(args["client_id"], is_notebook=False)
    ))
    args.pop("client_id")
    timer_start = datetime.datetime.now()
    df = viesav.save_historical_data_with_IB_loop(**args)
    args["ib"].disconnect()
    timer_stop = datetime.datetime.now()
    _LOG.info(
        "Finished. Time: %s seconds. Dataframe: %s",
        (timer_stop - timer_start).total_seconds(),
        vieuti.get_df_signature(df),
    )
    hio.delete_file(file_name)


def _run_download_data_IB_loop():
    # Run loading by chunks different symbol sequentially.
    symbols = ["DA", "ES", "NQ"]
    _LOG.info("Running sequential downloading of %s", symbols)
    timer_start = datetime.datetime.now()
    for symbol in symbols:
        _run_download_data_IB_loop_by_symbol(symbol) 
    timer_stop = datetime.datetime.now()
    _LOG.info("Finished %s sequential exctracting. Time: %s", symbols, (timer_stop - timer_start).total_seconds())
    # Run loading by chunks different symbols in parallel.
    _LOG.info("Running parallel downloading of %s", symbols)
    timer_start = datetime.datetime.now()
    joblib.Parallel(n_jobs=4)(joblib.delayed(_run_download_data_IB_loop_by_symbol)(symbol) for symbol in symbols)
    timer_stop = datetime.datetime.now()
    _LOG.info("Finished %s parallel exctracting. Time: %s", symbols, (timer_stop - timer_start).total_seconds())
 

def _run_download_data_IB_loop_by_symbol(symbol: str) -> None:
    # Run loading by chunks.
    _LOG.info("Running download_data_IB_loop code for %s symbol...", symbol)
    file_name = "./simple_ib_loop/experiment.csv"
    hio.create_enclosing_dir(file_name, incremental=True)
    _LOG.info("Starting in memory data extraction.")
    args = SETUP.copy()
    args.update(dict(
        ib=vieuti.ib_connect(sum(bytes(symbol, encoding="UTF-8")), is_notebook=False)
    ))
    args.update(dict(
        contract=args["ib"].qualifyContracts(ib_insync.ContFuture(symbol, exchange="GLOBEX", currency="USD"))[0],
    ))
    args.pop("client_id")
    for duration_str in ["1 D", "2 D"]:
        args.update(dict(duration_str=duration_str))
        _LOG.info("Run for %s duration", duration_str)
        timer_start = datetime.datetime.now()
        df = viedow.get_historical_data_with_IB_loop(**args)
        timer_stop = datetime.datetime.now()
        _LOG.info(
            "Finished. Time: %s seconds. Dataframe: %s",
            (timer_stop - timer_start).total_seconds(),
            vieuti.get_df_signature(df),
        )
    args["ib"].disconnect()
    hio.delete_file(file_name)


def _main(parser: argparse.ArgumentParser):
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    # _run_unrolling_download_data_ib_loop()
    # _run_save_historical_data_with_IB_loop()
    # _run_download_data_IB_loop()
    _run_req_wrapper()


def _parse():
    parser = argparse.ArgumentParser()
    hparse.add_verbosity_arg(parser)
    return parser


if __name__ == "__main__":
    _main(_parse())

# Sep 20    
# 03-16_20:58 [36mINFO [0m: _run_unrolling_download_data_ib_loop: Running unrolling_download_data_ib_loop code...
# 03-16_20:58 [36mINFO [0m: _run_unrolling_download_data_ib_loop: Starting data extraction. Mode: in_memory. Threads: serial.
# 03-16_21:06 [36mINFO [0m: _run_unrolling_download_data_ib_loop: Finished. Time: 493.179828 seconds. Dataframe: len=29805 [2020-09-01 00:00:00-04:00, 2020-09-30 23:59:00-04:00]
# 03-16_21:06 [36mINFO [0m: _run_unrolling_download_data_ib_loop: Starting data extraction. Mode: on_disk. Threads: serial.
# 03-16_21:15 [36mINFO [0m: _run_unrolling_download_data_ib_loop: Finished. Time: 528.675238 seconds. Dataframe: len=29805 [2020-09-01 00:00:00-04:00, 2020-09-30 23:59:00-04:00]
# 03-16_21:15 [36mINFO [0m: _run_unrolling_download_data_ib_loop: Starting data extraction. Mode: on_disk. Threads: 4.
# 03-16_21:23 [36mINFO [0m: _run_unrolling_download_data_ib_loop: Finished. Time: 443.870279 seconds. Dataframe: len=21840 [2020-09-01 00:00:00-04:00, 2020-09-30 23:59:00-04:00]
# 03-16_21:23 [36mINFO [0m: _run_save_historical_data_with_IB_loop: Running save_historical_data_with_IB_loop code...
# 03-16_21:23 [36mINFO [0m: _run_save_historical_data_with_IB_loop: Starting data extraction.
# 03-16_21:29 [36mINFO [0m: _run_save_historical_data_with_IB_loop: Finished. Time: 396.476231 seconds. Dataframe: len=29805 [2020-09-01 00:00:00-04:00, 2020-09-30 23:59:00-04:00]
# 03-16_21:29 [36mINFO [0m: _run_download_data_IB_loop: Running download_data_IB_loop code...
# 03-16_21:29 [36mINFO [0m: _run_download_data_IB_loop: Starting in memory data extraction.
# 03-16_21:35 [36mINFO [0m: _run_download_data_IB_loop: Finished. Time: 365.110541 seconds. Dataframe: len=29805 [2020-09-01 00:00:00-04:00, 2020-09-30 23:59:00-04:00]
# 03-16_21:35 [36mINFO [0m: _run_download_data_IB_loop: Starting to file data extraction.
# 03-16_21:41 [36mINFO [0m: _run_download_data_IB_loop: Finished. Time: 366.961852 seconds. Dataframe: 

# Feb Mar 21
# 03-16_21:45 [36mINFO [0m: _run_unrolling_download_data_ib_loop: Running unrolling_download_data_ib_loop code...
# 03-16_21:45 [36mINFO [0m: _run_unrolling_download_data_ib_loop: Starting data extraction. Mode: in_memory. Threads: serial.
# 03-16_21:49 [36mINFO [0m: _run_unrolling_download_data_ib_loop: Finished. Time: 223.140918 seconds. Dataframe: len=27434 [2021-02-14 18:00:00-05:00, 2021-03-14 23:59:00-04:00]
# 03-16_21:49 [36mINFO [0m: _run_unrolling_download_data_ib_loop: Starting data extraction. Mode: on_disk. Threads: serial.
# 03-16_21:53 [36mINFO [0m: _run_unrolling_download_data_ib_loop: Finished. Time: 236.328374 seconds. Dataframe: len=27434 [2021-02-14 18:00:00-05:00, 2021-03-14 23:59:00-04:00]
# 03-16_21:53 [36mINFO [0m: _run_unrolling_download_data_ib_loop: Starting data extraction. Mode: on_disk. Threads: 4.
# 03-16_21:57 [36mINFO [0m: _run_unrolling_download_data_ib_loop: Finished. Time: 232.805387 seconds. Dataframe: len=27434 [2021-02-14 18:00:00-05:00, 2021-03-14 23:59:00-04:00]
# 03-16_21:57 [36mINFO [0m: _run_save_historical_data_with_IB_loop: Running save_historical_data_with_IB_loop code...
# 03-16_21:57 [36mINFO [0m: _run_save_historical_data_with_IB_loop: Starting data extraction.
# 03-16_21:59 [36mINFO [0m: _run_save_historical_data_with_IB_loop: Finished. Time: 172.968709 seconds. Dataframe: len=27435 [2021-02-14 18:00:00-05:00, 2021-03-14 23:59:00-04:00]
# 03-16_21:59 [36mINFO [0m: _run_download_data_IB_loop: Running download_data_IB_loop code...
# 03-16_21:59 [36mINFO [0m: _run_download_data_IB_loop: Starting in memory data extraction.
# 03-16_22:02 [36mINFO [0m: _run_download_data_IB_loop: Finished. Time: 173.33158 seconds. Dataframe: len=27435 [2021-02-14 18:00:00-05:00, 2021-03-14 23:59:00-04:00]
# 03-16_22:02 [36mINFO [0m: _run_download_data_IB_loop: Starting to file data extraction.


# 03-17_09:09 [36mINFO [0m: _run_save_historical_data_with_IB_loop: Running save_historical_data_with_IB_loop code...
# 03-17_09:09 [36mINFO [0m: _run_save_historical_data_with_IB_loop: Starting data extraction.
# 03-17_09:20 [36mINFO [0m: _run_save_historical_data_with_IB_loop: Finished. Time: 668.162257 seconds. Dataframe: len=27660 [2019-06-02 18:00:00-04:00, 2019-06-30 23:59:00-04:00]
# 03-17_09:20 [36mINFO [0m: _run_download_data_IB_loop: Running download_data_IB_loop code...
# 03-17_09:20 [36mINFO [0m: _run_download_data_IB_loop: Starting in memory data extraction.
# 03-17_09:32 [36mINFO [0m: _run_download_data_IB_loop: Running download_data_IB_loop code...
# 03-17_09:32 [36mINFO [0m: _run_download_data_IB_loop: Starting in memory data extraction.
# 03-17_09:43 [36mINFO [0m: _run_download_data_IB_loop: Finished. Time: 667.468614 seconds. Dataframe: len=27660 [2019-06-02 18:00:00-04:00, 2019-06-30 23:59:00-04:00]
# 03-17_09:43 [36mINFO [0m: _run_download_data_IB_loop: Starting to file data extraction.

# 03-17_11:31 INFO : _run_download_data_IB_loop: Running download_data_IB_loop code...
# 03-17_11:31 INFO : _run_download_data_IB_loop: Starting in memory data extraction.
# 03-17_11:32 INFO : _run_download_data_IB_loop: Finished. Time: 60.418652 seconds. Dataframe: len=3735 [2020-02-12 00:00:00-05:00, 2020-02-14 16:59:00-05:00]
# 03-17_11:41 INFO : _run_download_data_IB_loop: Running download_data_IB_loop code...
# 03-17_11:41 INFO : _run_download_data_IB_loop: Starting in memory data extraction.
# 03-17_11:42 INFO : _run_download_data_IB_loop: Finished. Time: 60.199417 seconds. Dataframe: len=2730 [2020-03-12 00:00:00-04:00, 2020-03-15 23:59:00-04:00]
# 03-17_11:42 INFO : _run_download_data_IB_loop: Running download_data_IB_loop code...
# 03-17_11:42 INFO : _run_download_data_IB_loop: Starting in memory data extraction.
# 03-17_11:43 INFO : _run_download_data_IB_loop: Finished. Time: 59.566148 seconds. Dataframe: len=2730 [2020-03-12 00:00:00-04:00, 2020-03-15 23:59:00-04:00]
# 03-17_11:43 INFO : _run_download_data_IB_loop: Running download_data_IB_loop code...
# 03-17_11:43 INFO : _run_download_data_IB_loop: Starting in memory data extraction.
# 03-17_11:44 INFO : _run_download_data_IB_loop: Finished. Time: 65.266076 seconds. Dataframe: len=2730 [2020-03-12 00:00:00-04:00, 2020-03-15 23:59:00-04:00]

# 03-17_11:33 INFO : _run_download_data_IB_loop: Running download_data_IB_loop code...
# 03-17_11:33 INFO : _run_download_data_IB_loop: Starting in memory data extraction.
# 03-17_11:33 INFO : _run_download_data_IB_loop: Finished. Time: 6.145032 seconds. Dataframe: len=2505 [2021-02-12 00:00:00-05:00, 2021-02-15 23:59:00-05:00]
# 03-17_11:33 INFO : _run_download_data_IB_loop: Running download_data_IB_loop code...
# 03-17_11:33 INFO : _run_download_data_IB_loop: Starting in memory data extraction.
# 03-17_11:33 INFO : _run_download_data_IB_loop: Finished. Time: 10.993462 seconds. Dataframe: len=2505 [2021-02-12 00:00:00-05:00, 2021-02-15 23:59:00-05:00]
# 03-17_11:34 INFO : _run_download_data_IB_loop: Running download_data_IB_loop code...
# 03-17_11:34 INFO : _run_download_data_IB_loop: Starting in memory data extraction.
# 03-17_11:34 INFO : _run_download_data_IB_loop: Finished. Time: 27.771231 seconds. Dataframe: len=2505 [2021-02-12 00:00:00-05:00, 2021-02-15 23:59:00-05:00]
# 03-17_11:37 INFO : _run_download_data_IB_loop: Running download_data_IB_loop code...
# 03-17_11:37 INFO : _run_download_data_IB_loop: Starting in memory data extraction.
# 03-17_11:38 INFO : _run_download_data_IB_loop: Finished. Time: 6.145862 seconds. Dataframe: len=2505 [2021-02-12 00:00:00-05:00, 2021-02-15 23:59:00-05:00]

# 03-17_11:39 INFO : _run_download_data_IB_loop: Running download_data_IB_loop code...
# 03-17_11:39 INFO : _run_download_data_IB_loop: Starting in memory data extraction.
# 03-17_11:39 INFO : _run_download_data_IB_loop: Finished. Time: 0.45863 seconds. Dataframe: len=2730 [2021-03-12 00:00:00-05:00, 2021-03-15 23:59:00-04:00]
# 03-17_11:39 INFO : _run_download_data_IB_loop: Running download_data_IB_loop code...
# 03-17_11:39 INFO : _run_download_data_IB_loop: Starting in memory data extraction.
# 03-17_11:39 INFO : _run_download_data_IB_loop: Finished. Time: 0.448817 seconds. Dataframe: len=2730 [2021-03-12 00:00:00-05:00, 2021-03-15 23:59:00-04:00]
# 03-17_11:39 INFO : _run_download_data_IB_loop: Running download_data_IB_loop code...
# 03-17_11:39 INFO : _run_download_data_IB_loop: Starting in memory data extraction.
# 03-17_11:39 INFO : _run_download_data_IB_loop: Finished. Time: 0.427792 seconds. Dataframe: len=2730 [2021-03-12 00:00:00-05:00, 2021-03-15 23:59:00-04:00]


# 03-17_13:22 INFO : _run_download_data_IB_loop: Running download_data_IB_loop code...
# 03-17_13:22 INFO : _run_download_data_IB_loop: Starting in memory data extraction.
# 03-17_13:24 ERROR: error          : Error 162, reqId 5: Historical Market Data Service error message:API historical data query cancelled: 5, contract: ContFuture(conId=412889032, symbol='ES', lastTradeDateOrContractMonth='20210618', multiplier='50', exchange='GLOBEX', currency='USD', localSymbol='ESM1', tradingClass='ES')
# 03-17_13:25 ERROR: error          : Error 162, reqId 6: Historical Market Data Service error message:API historical data query cancelled: 6, contract: ContFuture(conId=412889032, symbol='ES', lastTradeDateOrContractMonth='20210618', multiplier='50', exchange='GLOBEX', currency='USD', localSymbol='ESM1', tradingClass='ES')
# 03-17_13:26 ERROR: error          : Error 162, reqId 7: Historical Market Data Service error message:API historical data query cancelled: 7, contract: ContFuture(conId=412889032, symbol='ES', lastTradeDateOrContractMonth='20210618', multiplier='50', exchange='GLOBEX', currency='USD', localSymbol='ESM1', tradingClass='ES')
# 03-17_13:27 ERROR: error          : Error 162, reqId 8: Historical Market Data Service error message:API historical data query cancelled: 8, contract: ContFuture(conId=412889032, symbol='ES', lastTradeDateOrContractMonth='20210618', multiplier='50', exchange='GLOBEX', currency='USD', localSymbol='ESM1', tradingClass='ES')
# 03-17_13:28 ERROR: error          : Error 162, reqId 9: Historical Market Data Service error message:API historical data query cancelled: 9, contract: ContFuture(conId=412889032, symbol='ES', lastTradeDateOrContractMonth='20210618', multiplier='50', exchange='GLOBEX', currency='USD', localSymbol='ESM1', tradingClass='ES')
# 03-17_13:29 INFO : _run_download_data_IB_loop: Finished. Time: 416.615881 seconds. Dataframe: len=8550 [2021-03-07 18:00:00-05:00, 2021-03-15 23:59:00-04:00]
# 03-17_13:29 INFO : _run_download_data_IB_loop: Run for 4 D duration
# 03-17_13:30 INFO : _run_download_data_IB_loop: Finished. Time: 65.863559 seconds. Dataframe: len=15015 [2021-03-01 00:00:00-05:00, 2021-03-15 23:59:00-04:00]
# 03-17_13:30 INFO : _run_download_data_IB_loop: Run for 2 D duration
# 03-17_13:31 INFO : _run_download_data_IB_loop: Finished. Time: 66.181277 seconds. Dataframe: len=15015 [2021-03-01 00:00:00-05:00, 2021-03-15 23:59:00-04:00]
# 03-17_13:31 INFO : _run_download_data_IB_loop: Run for 1 D duration
# 03-17_13:32 INFO : _run_download_data_IB_loop: Finished. Time: 65.74733 seconds. Dataframe: len=15015 [2021-03-01 00:00:00-05:00, 2021-03-15 23:59:00-04:00]


# 03-17_13:48 INFO : _run_download_data_IB_loop: Running download_data_IB_loop code...
# 03-17_13:48 INFO : _run_download_data_IB_loop: Starting in memory data extraction.
# 03-17_13:48 INFO : _run_download_data_IB_loop: Run for 4 D duration
# 03-17_13:49 ERROR: error          : Error 162, reqId 4: Historical Market Data Service error message:API historical data query cancelled: 4, contract: ContFuture(conId=412889032, symbol='ES', lastTradeDateOrContractMonth='20210618', multiplier='50', exchange='GLOBEX', currency='USD', localSymbol='ESM1', tradingClass='ES')
# 03-17_13:51 INFO : _run_download_data_IB_loop: Running download_data_IB_loop code...
# 03-17_13:51 INFO : _run_download_data_IB_loop: Starting in memory data extraction.
# 03-17_13:51 INFO : _run_download_data_IB_loop: Run for 2 D duration
# 03-17_13:58 INFO : _run_download_data_IB_loop: Finished. Time: 457.563885 seconds. Dataframe: len=27660 [2020-03-01 18:00:00-05:00, 2020-03-29 23:59:00-04:00]
# 03-17_13:58 INFO : _run_download_data_IB_loop: Run for 1 D duration
# 03-17_14:07 INFO : _run_download_data_IB_loop: Finished. Time: 533.918162 seconds. Dataframe: len=27660 [2020-03-01 18:00:00-05:00, 2020-03-29 23:59:00-04:00]
# 03-17_14:13 INFO : _run_download_data_IB_loop: Run for 3 D duration
# 03-17_14:15 ERROR: error          : Error 162, reqId 5: Historical Market Data Service error message:API historical data query cancelled: 5, contract: ContFuture(conId=412889032, symbol='ES', lastTradeDateOrContractMonth='20210618', multiplier='50', exchange='GLOBEX', currency='USD', localSymbol='ESM1', tradingClass='ES')
# 03-17_14:19 INFO : _run_download_data_IB_loop: Run for 1 D duration
# 03-17_14:27 INFO : _run_download_data_IB_loop: Finished. Time: 468.14182 seconds. Dataframe: len=27300 [2020-04-01 00:00:00-04:00, 2020-04-29 23:59:00-04:00]
# 03-17_14:27 INFO : _run_download_data_IB_loop: Run for 2 D duration
# 03-17_14:35 INFO : _run_download_data_IB_loop: Finished. Time: 492.226023 seconds. Dataframe: len=27300 [2020-04-01 00:00:00-04:00, 2020-04-29 23:59:00-04:00]


# 03-17_15:36 INFO : _run_req_wrapper: Running request wrapper code 1 M 1 times...
# 03-17_15:37 INFO : _run_req_wrapper: Finished. Time: 60.041627 seconds. Dataframe:
# 03-17_15:37 INFO : _run_req_wrapper: Running request wrapper code 1 D 30 times...
# 03-17_15:37 ERROR: error          : Error 162, reqId 4: Historical Market Data Service error message:API historical data query cancelled: 4, contract: ContFuture(conId=412889032, symbol='ES', lastTradeDateOrContractMonth='20210618', multiplier='50', exchange='GLOBEX', currency='USD', localSymbol='ESM1', tradingClass='ES')
# 03-17_15:40 INFO : _run_req_wrapper: Finished. Time: 180.234238 seconds. Dataframe: len=1365 [2021-01-28 18:00:00-05:00, 2021-01-29 16:59:00-05:00]
# 03-17_15:53 INFO : _run_req_wrapper: Running request wrapper code 1 M 1 times...
# 03-17_15:54 INFO : _run_req_wrapper: Finished. Time: 60.054174 seconds. Dataframe:
# 03-17_15:54 INFO : _run_req_wrapper: Running request wrapper code 10 D 3 times...
# 03-17_15:57 INFO : _run_req_wrapper: Finished. Time: 180.169242 seconds. Dataframe:
# 03-17_15:57 INFO : _run_req_wrapper: Running request wrapper code 1 D 30 times...
# 03-17_16:00 INFO : _run_req_wrapper: Finished. Time: 180.323943 seconds. Dataframe: len=1365 [2021-01-28 18:00:00-05:00, 2021-01-29 16:59:00-05:00]


# I turned off timeout
#  03-17_16:22 INFO : _run_req_wrapper: Running request wrapper code 1 M 1 times...
#  03-17_16:27 INFO : _run_req_wrapper: Finished. Time: 265.133249 seconds. Dataframe: len=40305 [2020-11-18 18:00:00-05:00, 2020-12-31 16:59:00-05:00]
#  03-17_16:27 INFO : _run_req_wrapper: Running request wrapper code 10 D 3 times...
#  03-17_16:31 INFO : _run_req_wrapper: Finished. Time: 281.333677 seconds. Dataframe: len=13440 [2020-12-16 18:00:00-05:00, 2020-12-31 16:59:00-05:00]
#  03-17_16:31 INFO : _run_req_wrapper: Running request wrapper code 1 D 30 times...
#  03-17_16:34 INFO : _run_req_wrapper: Finished. Time: 185.825444 seconds. Dataframe: len=1365 [2021-01-28 18:00:00-05:00, 2021-01-29 16:59:00-05:00]

# I select end date as today
# 03-17_16:37 INFO : _run_req_wrapper: Running request wrapper code 1 M 1 times...
# 03-17_16:40 INFO : _run_req_wrapper: Finished. Time: 133.045644 seconds. Dataframe: len=28800 [2021-02-14 18:00:00-05:00, 2021-03-15 23:59:00-04:00]
# 03-17_16:40 INFO : _run_req_wrapper: Running request wrapper code 10 D 1 times...
# 03-17_16:41 INFO : _run_req_wrapper: Finished. Time: 59.563378 seconds. Dataframe: len=12645 [2021-03-02 18:00:00-05:00, 2021-03-15 23:59:00-04:00]
# 03-17_16:41 INFO : _run_req_wrapper: Running request wrapper code 1 D 1 times...
# 03-17_16:41 INFO : _run_req_wrapper: Finished. Time: 0.138153 seconds. Dataframe: len=360 [2021-03-15 18:00:00-04:00, 2021-03-15 23:59:00-04:00]

# 03-17_18:45 INFO : _run_req_wrapper: Running request wrapper code 1 M 1 times...
# 03-17_18:55 INFO : _run_req_wrapper: Finished. Time: 599.163548 seconds. Dataframe: len=27435 [2020-02-16 18:00:00-05:00, 2020-03-15 23:59:00-04:00]
# 03-17_18:55 INFO : _run_req_wrapper: Running request wrapper code 10 D 3 times...
# 03-17_19:06 INFO : _run_req_wrapper: Finished. Time: 653.74148 seconds. Dataframe: len=12645 [2020-03-02 18:00:00-05:00, 2020-03-15 23:59:00-04:00]
# 03-17_19:06 INFO : _run_req_wrapper: Running request wrapper code 1 D 30 times...
# 03-17_19:13 INFO : _run_req_wrapper: Finished. Time: 431.639991 seconds. Dataframe: len=360 [2020-03-15 18:00:00-04:00, 2020-03-15 23:59:00-04:00]

# 03-18_12:38 INFO : _run_download_data_IB_loop: Running parallel downloading of ['DA', 'ES', 'NQ']
# 03-18_12:50 INFO : _run_download_data_IB_loop: Finished ['DA', 'ES', 'NQ'] parallel exctracting. Time: 722.238344
# 03-18_14:16 INFO : _run_download_data_IB_loop: Running sequential downloading of ['DA', 'ES', 'NQ']
# 03-18_14:28 INFO : _run_download_data_IB_loop: Finished ['DA', 'ES', 'NQ'] sequential exctracting. Time: 722.349846


# 03-18_14:16 INFO : _run_download_data_IB_loop_by_symbol: Running download_data_IB_loop code for DA symbol...
# 03-18_14:16 INFO : _run_download_data_IB_loop_by_symbol: Starting in memory data extraction.
# 03-18_14:16 INFO : _run_download_data_IB_loop_by_symbol: Run for 1 D duration
# 03-18_14:18 INFO : _run_download_data_IB_loop_by_symbol: Finished. Time: 122.271113 seconds. Dataframe: len=3119 [2020-08-03 18:01:00-04:00, 2020-08-05 23:59:00-04:00]
# 03-18_14:18 INFO : _run_download_data_IB_loop_by_symbol: Run for 2 D duration
# 03-18_14:21 INFO : _run_download_data_IB_loop_by_symbol: Finished. Time: 197.864097 seconds. Dataframe: len=5513 [2020-08-02 18:06:00-04:00, 2020-08-06 16:59:00-04:00]
# 03-18_14:21 INFO : _run_download_data_IB_loop_by_symbol: Running download_data_IB_loop code for ES symbol...
# 03-18_14:21 INFO : _run_download_data_IB_loop_by_symbol: Starting in memory data extraction.
# 03-18_14:21 INFO : _run_download_data_IB_loop_by_symbol: Run for 1 D duration
# 03-18_14:23 INFO : _run_download_data_IB_loop_by_symbol: Finished. Time: 107.974775 seconds. Dataframe: len=5820 [2020-08-02 18:00:00-04:00, 2020-08-06 23:59:00-04:00]
# 03-18_14:23 INFO : _run_download_data_IB_loop_by_symbol: Run for 2 D duration
# 03-18_14:25 INFO : _run_download_data_IB_loop_by_symbol: Finished. Time: 114.033619 seconds. Dataframe: len=5820 [2020-08-02 18:00:00-04:00, 2020-08-06 23:59:00-04:00]
# 03-18_14:25 INFO : _run_download_data_IB_loop_by_symbol: Running download_data_IB_loop code for NQ symbol...
# 03-18_14:25 INFO : _run_download_data_IB_loop_by_symbol: Starting in memory data extraction.
# 03-18_14:25 INFO : _run_download_data_IB_loop_by_symbol: Run for 1 D duration
# 03-18_14:26 INFO : _run_download_data_IB_loop_by_symbol: Finished. Time: 83.811099 seconds. Dataframe: len=5820 [2020-08-02 18:00:00-04:00, 2020-08-06 23:59:00-04:00]
# 03-18_14:26 INFO : _run_download_data_IB_loop_by_symbol: Run for 2 D duration
# 03-18_14:28 INFO : _run_download_data_IB_loop_by_symbol: Finished. Time: 96.144792 seconds. Dataframe: len=5820 [2020-08-02 18:00:00-04:00, 2020-08-06 23:59:00-04:00]

