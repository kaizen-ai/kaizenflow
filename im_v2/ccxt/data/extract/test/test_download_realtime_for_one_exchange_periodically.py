from datetime import datetime, timedelta

import pytest

import helpers.hsystem as hsystem
import helpers.hunit_test as hunitest


@pytest.mark.skip(reason="Replace with smoke test in CmTask #2083")
class TestDownloadRealtimeForOneExchangePeriodically1(hunitest.TestCase):
    def test_amount_of_downloads(self) -> None:
        """
        Test Python script call, check return value and amount of downloads.
        """
        cmd = "im_v2/ccxt/data/extract/download_realtime_for_one_exchange_periodically.py \
        --data_type 'ohlcv' \
        --exchange_id 'binance' \
        --universe 'small' \
        --db_stage 'dev' \
        --db_table 'ccxt_ohlcv_test' \
        --aws_profile 'ck' \
        --s3_path 's3://cryptokaizen-data-test/realtime/' \
        --interval_min '1' \
        --start_time '{start_time}' \
        --stop_time '{stop_time}' \
        --method 'rest'"
        start_delay = 0
        stop_delay = 1
        # Amount of downloads depends on the start time and stop time.
        current_time = datetime.now()
        start_time = current_time + timedelta(minutes=start_delay, seconds=30)
        stop_time = current_time + timedelta(minutes=stop_delay, seconds=30)
        # Call Python script in order to get output.
        cmd = cmd.format(
            start_time=start_time.strftime("%Y-%m-%d %H:%M:%S"),
            stop_time=stop_time.strftime("%Y-%m-%d %H:%M:%S"),
        )
        return_code, output = hsystem.system_to_string(cmd)
        # Check return value.
        self.assertEqual(return_code, 0)
        # Check amount of downloads by parsing output.
        download_started_marker = "Starting data download from"
        self.assertEqual(output.count(download_started_marker), 1)
        download_completed_marker = "Successfully completed, iteration took"
        self.assertEqual(output.count(download_completed_marker), 1)
