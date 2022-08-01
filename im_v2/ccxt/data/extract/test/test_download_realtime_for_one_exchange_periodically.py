from datetime import datetime, timedelta

import pytest
import logging

import helpers.henv as henv
import helpers.hsystem as hsystem
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)
_LOG.setLevel(logging.INFO)


@pytest.mark.skipif(
    not henv.execute_repo_config_code("is_CK_S3_available()"),
    reason="Run only if CK S3 is available",
)
class TestDownloadRealtimeForOneExchangePeriodically1(hunitest.TestCase):
    @pytest.mark.superslow("~40 seconds.")
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
        --stop_time '{stop_time}'"
        start_delay = 0
        stop_delay = 1
        download_started_marker = "Starting data download"
        # Amount of downloads depends on the start time and stop time.
        expected_downloads_amount = 1
        current_time = datetime.now()
        start_time = current_time + timedelta(minutes=start_delay, seconds=30)
        stop_time = current_time + timedelta(minutes=stop_delay, seconds=30)
        # Call Python script in order to get output.
        cmd = cmd.format(
            start_time=start_time.strftime("%Y-%m-%d %H:%M:%S"),
            stop_time=stop_time.strftime("%Y-%m-%d %H:%M:%S"),
        )
        return_code, output = hsystem.system_to_string(cmd, log_level=logging.INFO)
        from pdb import set_trace; set_trace()
        # Check return value.
        self.assertEqual(return_code, 0)
        # Check amount of downloads by parsing output.
        self.assertEqual(
            output.count(download_started_marker), expected_downloads_amount
        )
