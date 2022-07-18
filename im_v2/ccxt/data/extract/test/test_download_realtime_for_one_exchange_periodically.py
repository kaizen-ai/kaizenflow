from datetime import datetime, timedelta

import pytest

import helpers.henv as henv
import helpers.hsystem as hsystem
import helpers.hunit_test as hunitest

import logging
_LOG = logging.getLogger(__name__)


@pytest.mark.skipif(
    not henv.execute_repo_config_code("is_CK_S3_available()"),
    reason="Run only if CK S3 is available",
)
class TestDownloadRealtimeForOneExchangePeriodically1(hunitest.TestCase):
    @pytest.mark.slow
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
        download_finished_marker = "Starting data download"
        # Amount of downloads depends on the start time and stop time.
        expected_downloads_amount = 1
        start_time = datetime.now() + timedelta(minutes=start_delay, seconds=5)
        stop_time = datetime.now() + timedelta(minutes=stop_delay, seconds=5)
        # Call Python script in order to get output.
        cmd = cmd.format(
            start_time=start_time.strftime("%Y-%m-%d %H:%M:%S"),
            stop_time=stop_time.strftime("%Y-%m-%d %H:%M:%S"))
        return_code, output = hsystem.system_to_string(cmd)
        _LOG.warning(output)
        # Check return value.
        self.assertEqual(return_code, 0)
        # Check amount of downloads by parsing output.
        self.assertEqual(
            output.count(download_finished_marker), expected_downloads_amount
        )
