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
    start_ts=pd.Timestamp(year=2020, month=9, day=1),
    end_ts=pd.Timestamp(year=2021, month=3, day=1),
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
