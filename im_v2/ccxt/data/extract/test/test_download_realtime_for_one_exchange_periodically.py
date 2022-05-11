import subprocess
from datetime import datetime, timedelta

import pytest

import helpers.hgit as hgit
import helpers.hunit_test as hunitest


@pytest.mark.skipif(
    not hgit.execute_repo_config_code("is_CK_S3_available()"),
    reason="Run only if CK S3 is available",
)
class TestDownloadRealtimeForOneExchangePeriodically1(hunitest.TestCase):
    def test_amount_of_downloads(self) -> None:
        """
        Test python script call, check return value and amount of downloads.
        """
        cmd = "im_v2/ccxt/data/extract/download_realtime_for_one_exchange_periodically.py \
        --exchange_id 'binance' \
        --universe 'v3' \
        --db_stage 'dev' \
        --db_table 'ccxt_ohlcv_test' \
        --aws_profile 'ck' \
        --s3_path 's3://cryptokaizen-data-test/realtime/' \
        --interval_min '1' \
        --start_time '{start_time}' \
        --stop_time '{stop_time}'"
        start_delay = 0
        stop_delay = 1
        download_started_marker = "Starting data download"
        # Amount of downloads depends on the start time and stop time.
        expected_downloads_amount = stop_delay - start_delay
        start_time = datetime.now() + timedelta(minutes=start_delay, seconds=5)
        stop_time = datetime.now() + timedelta(minutes=stop_delay, seconds=5)
        # Call python script in order to get output.
        cmd = cmd.format(
            start_time=start_time.strftime("%Y-%m-%d %H:%M:%S"),
            stop_time=stop_time.strftime("%Y-%m-%d %H:%M:%S"),
        )
        process = subprocess.Popen(
            args=cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True
        )
        stdout, _ = process.communicate()
        # Check return value.
        self.assertEqual(process.returncode, 0)
        # Check amount of downloads by parsing output.
        self.assertEqual(
            str(stdout).count(download_started_marker), expected_downloads_amount
        )
