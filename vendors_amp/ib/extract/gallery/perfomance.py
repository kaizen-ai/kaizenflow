#!/usr/bin/env python

import argparse
import datetime
import logging

import ib_insync
import pandas as pd

import helpers.dbg as dbg
import helpers.io_ as hio
import helpers.parser as hparse
import vendors_amp.ib.extract.download_data_ib_loop as viedow
import vendors_amp.ib.extract.save_historical_data_with_IB_loop as viesav
import vendors_amp.ib.extract.unrolling_download_data_ib_loop as vieunr
import vendors_amp.ib.extract.utils as vieuti

_LOG = logging.getLogger(__name__)

SETUP = dict(
    client_id=100,
    contract=ib_insync.ContFuture(symbol="ES", exchange="GLOBEX", currency="USD"),
    start_ts=pd.Timestamp(year=2021, month=2, day=14),
    end_ts=pd.Timestamp(year=2021, month=3, day=15),
    duration="1 D",
    bar_size="1 min",
    what_to_show="TRADES",
    use_rth=False,
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
        _LOG.info(
            "Starting data extraction. Mode: %s. Threads: %s.", mode, num_threads
        )
        timer_start = datetime.datetime.now()
        df = vieunr.get_historical_data(
            client_id=SETUP["client_id"],
            contract=SETUP["contract"],
            start_ts=SETUP["start_ts"],
            end_ts=SETUP["end_ts"],
            bar_size_setting=SETUP["bar_size"],
            what_to_show=SETUP["what_to_show"],
            use_rth=SETUP["use_rth"],
            mode=mode,
            num_threads=num_threads,
            incremental=False,
            dst_dir=dst_dir,
        )
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
    timer_start = datetime.datetime.now()
    ib = vieuti.ib_connect(SETUP["client_id"], is_notebook=False)
    df = viesav.save_historical_data_with_IB_loop(
        ib=ib,
        contract=SETUP["contract"],
        start_ts=SETUP["start_ts"],
        end_ts=SETUP["end_ts"],
        duration_str=SETUP["duration"],
        bar_size_setting=SETUP["bar_size"],
        what_to_show=SETUP["what_to_show"],
        use_rth=SETUP["use_rth"],
        file_name=file_name,
    )
    ib.disconnect()
    timer_stop = datetime.datetime.now()
    _LOG.info(
        "Finished. Time: %s seconds. Dataframe: %s",
        (timer_stop - timer_start).total_seconds(),
        vieuti.get_df_signature(df),
    )
    hio.delete_file(file_name)


def _run_download_data_IB_loop():
    # Run saving to separate files.
    _LOG.info("Running download_data_IB_loop code...")
    file_name = "./simple_ib_loop/experiment.csv"
    hio.create_enclosing_dir(file_name, incremental=True)
    _LOG.info("Starting in memory data extraction.")
    timer_start = datetime.datetime.now()
    ib = vieuti.ib_connect(SETUP["client_id"], is_notebook=False)
    contract = ib.qualifyContracts(SETUP["contract"])
    contract = contract[0]
    df = viedow.get_historical_data_with_IB_loop(
        ib=ib,
        contract=contract,
        start_ts=SETUP["start_ts"],
        end_ts=SETUP["end_ts"],
        duration_str=SETUP["duration"],
        bar_size_setting=SETUP["bar_size"],
        what_to_show=SETUP["what_to_show"],
        use_rth=SETUP["use_rth"],
    )
    ib.disconnect()
    timer_stop = datetime.datetime.now()
    _LOG.info(
        "Finished. Time: %s seconds. Dataframe: %s",
        (timer_stop - timer_start).total_seconds(),
        vieuti.get_df_signature(df),
    )
    _LOG.info("Starting to file data extraction.")
    timer_start = datetime.datetime.now()
    ib = vieuti.ib_connect(SETUP["client_id"], is_notebook=False)
    df = viedow.save_historical_data_single_file_with_IB_loop(
        ib=ib,
        contract=contract,
        start_ts=SETUP["start_ts"],
        end_ts=SETUP["end_ts"],
        duration_str=SETUP["duration"],
        bar_size_setting=SETUP["bar_size"],
        what_to_show=SETUP["what_to_show"],
        use_rth=SETUP["use_rth"],
        file_name=file_name,
        incremental=False,
    )
    ib.disconnect()
    timer_stop = datetime.datetime.now()
    _LOG.info(
        "Finished. Time: %s seconds. Dataframe: %s",
        (timer_stop - timer_start).total_seconds(),
        vieuti.get_df_signature(df),
    )
    hio.delete_file(file_name)


def _main(parser: argparse.ArgumentParser):
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    _run_unrolling_download_data_ib_loop()
    _run_save_historical_data_with_IB_loop()
    _run_download_data_IB_loop()


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
