import datetime
import os

import pandas as pd
import pytest

import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hgit as hgit
import helpers.hsystem as hsystem
import helpers.hunit_test as hunitest
import im_v2.talos.utils as imv2tauti


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
        amp_dir = hgit.get_amp_abs_path()
        cmd = "im_v2/talos/data/extract/download_realtime_for_one_exchange_periodically.py"
        cmd = os.path.join(amp_dir, cmd)
        hdbg.dassert_file_exists(cmd)
        #
        cmd += " \
        --data_type 'ohlcv' \
        --exchange_id 'binance' \
        --universe 'v1' \
        --db_stage 'dev' \
        --db_table 'talos_ohlcv_test' \
        --aws_profile 'ck' \
        --s3_path 's3://cryptokaizen-data-test/realtime/' \
        --interval_min '1' \
        --start_time '{start_time}' \
        --stop_time '{stop_time}' \
        --method 'rest'"
        start_delay = 0
        stop_delay = 1
        download_started_marker = "Starting data download"
        # Amount of downloads depends on the start time and stop time.
        expected_downloads_amount = stop_delay - start_delay
        start_time = pd.Timestamp.now(tz="UTC") + datetime.timedelta(
            minutes=start_delay, seconds=15
        )
        stop_time = pd.Timestamp.now(tz="UTC") + datetime.timedelta(
            minutes=stop_delay, seconds=15
        )
        # Call Python script in order to get output.
        cmd = cmd.format(
            start_time=imv2tauti.timestamp_to_talos_iso_8601(start_time),
            stop_time=imv2tauti.timestamp_to_talos_iso_8601(stop_time),
        )
        return_code, output = hsystem.system_to_string(cmd)
        # Check return value.
        self.assertEqual(return_code, 0)
        # Check amount of downloads by parsing output.
        self.assertEqual(
            output.count(download_started_marker), expected_downloads_amount
        )