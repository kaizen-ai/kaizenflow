import pprint

import helpers.hunit_test as hunitest
import im_v2.airflow.dags.airflow_utils.datapull.datapull_utils as imvadauddu


class Test_get_download_websocket_data_command(hunitest.TestCase):
    def test1(self) -> None:
        """
        Check that the method correctly handles input with all parameters.
        """
        # Prepare input data.
        components_dict = {
            "exchange_id": "binance",
            "universe": "v8",
            "data_type": "ohlcv",
            "contract_type": "spot",
            "vendor": "ccxt",
            "db_stage": "preprod",
            "start_time": "2021-01-01T00:00:00Z",
            "stop_time": "2021-01-01T00:01:00Z",
            "download_mode": "airflow",
            "downloading_entity": "realtime",
            "action_tag": "downloaded_200ms",
            "data_format": "postgres",
            "bid_ask_depth": "1",
            "websocket_data_buffer_size": "0",
            "chunk_size": "100",
            "id": "3",
        }
        expected = r"""
        ['/app/amp/im_v2/ccxt/data/extract/download_exchange_data_to_db_periodically.py',
        "--method 'websocket'",
        "--aws_profile 'ck'",
        '--exchange_id binance',
        '--universe v8',
        '--db_table ccxt_ohlcv_spot',
        '--data_type ohlcv',
        '--contract_type spot',
        '--vendor ccxt',
        '--db_stage preprod',
        "--start_time '2021-01-01T00:00:00Z'",
        "--stop_time '2021-01-01T00:01:00Z'",
        '--download_mode airflow',
        '--downloading_entity realtime',
        '--action_tag downloaded_200ms',
        '--data_format postgres',
        '--bid_ask_depth 1',
        '--websocket_data_buffer_size 0',
        '--universe_part 100 3']
        """
        actual = pprint.pformat(
            imvadauddu.get_download_websocket_data_command(components_dict)
        )
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test2(self) -> None:
        """
        Check that the method correctly handles input with all parameters
        except optional.
        """
        # Prepare input data.
        components_dict = {
            "exchange_id": "binance",
            "universe": "v8",
            "data_type": "ohlcv",
            "contract_type": "spot",
            "vendor": "ccxt",
            "db_stage": "preprod",
            "start_time": "2021-01-01T00:00:00Z",
            "stop_time": "2021-01-01T00:01:00Z",
            "download_mode": "airflow",
            "downloading_entity": "realtime",
            "action_tag": "downloaded_200ms",
            "data_format": "postgres",
        }
        expected = r"""
        ['/app/amp/im_v2/ccxt/data/extract/download_exchange_data_to_db_periodically.py',
        "--method 'websocket'",
        "--aws_profile 'ck'",
        '--exchange_id binance',
        '--universe v8',
        '--db_table ccxt_ohlcv_spot',
        '--data_type ohlcv',
        '--contract_type spot',
        '--vendor ccxt',
        '--db_stage preprod',
        "--start_time '2021-01-01T00:00:00Z'",
        "--stop_time '2021-01-01T00:01:00Z'",
        '--download_mode airflow',
        '--downloading_entity realtime',
        '--action_tag downloaded_200ms',
        '--data_format postgres']
        """
        actual = pprint.pformat(
            imvadauddu.get_download_websocket_data_command(components_dict)
        )
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test3(self) -> None:
        """
        Check that the method raises an error when a required parameter is
        missing.
        """
        # Prepare input data.
        components_dict = {
            "exchange_id": "binance",
            "universe": "v8",
            "data_type": "ohlcv",
            "contract_type": "spot",
            "vendor": "ccxt",
            "db_stage": "preprod",
            "download_mode": "airflow",
            "downloading_entity": "realtime",
            "action_tag": "downloaded_200ms",
            "data_format": "postgres",
        }
        with self.assertRaises(ValueError) as error:
            imvadauddu.get_download_websocket_data_command(components_dict)
        actual = str(error.exception)
        expected = r"""Missing required parameter: start_time"""
        self.assert_equal(actual, expected, fuzzy_match=True)


class Test_get_download_bulk_data_command(hunitest.TestCase):
    def test1(self) -> None:
        """
        Check that the method correctly handles input with all parameters.
        """
        # Prepare input data.
        components_dict = {
            "end_timestamp": "2021-01-01T00:00:00Z",
            "start_timestamp": "2021-01-01T00:01:00Z",
            "vendor": "ccxt",
            "exchange_id": "binance",
            "universe": "v8",
            "data_type": "ohlcv",
            "contract_type": "spot",
            "s3_path": "s3://bucket",
            "download_mode": "periodic_daily",
            "downloading_entity": "airflow",
            "action_tag": "downloaded_1min",
            "data_format": "parquet",
            "assert_on_missing_data": "",
            "version": "v2_0_0",
            "universe_part": "25",
            "id": "3",
        }
        expected = r"""
        ['/app/amp/im_v2/common/data/extract/download_bulk.py',
        "--aws_profile 'ck'",
        "--end_timestamp '2021-01-01T00:00:00Z'",
        "--start_timestamp '2021-01-01T00:01:00Z'",
        '--vendor ccxt',
        '--exchange_id binance',
        '--universe v8',
        '--data_type ohlcv',
        '--contract_type spot',
        '--s3_path s3://bucket',
        '--download_mode periodic_daily',
        '--downloading_entity airflow',
        '--action_tag downloaded_1min',
        '--data_format parquet',
        '--assert_on_missing_data ',
        '--version v2_0_0',
        '--universe_part 25 3']
        """
        actual = pprint.pformat(
            imvadauddu.get_download_bulk_data_command(components_dict)
        )
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test2(self) -> None:
        """
        Check that the method correctly handles input with all parameters
        except optional.
        """
        # Prepare input data.
        components_dict = {
            "end_timestamp": "2021-01-01T00:00:00Z",
            "start_timestamp": "2021-01-01T00:01:00Z",
            "vendor": "ccxt",
            "exchange_id": "binance",
            "universe": "v8",
            "data_type": "ohlcv",
            "contract_type": "spot",
            "s3_path": "s3://bucket",
            "download_mode": "periodic_daily",
            "downloading_entity": "airflow",
            "action_tag": "downloaded_1min",
            "data_format": "parquet",
        }
        expected = r"""
        ['/app/amp/im_v2/common/data/extract/download_bulk.py',
        "--aws_profile 'ck'",
        "--end_timestamp '2021-01-01T00:00:00Z'",
        "--start_timestamp '2021-01-01T00:01:00Z'",
        '--vendor ccxt',
        '--exchange_id binance',
        '--universe v8',
        '--data_type ohlcv',
        '--contract_type spot',
        '--s3_path s3://bucket',
        '--download_mode periodic_daily',
        '--downloading_entity airflow',
        '--action_tag downloaded_1min',
        '--data_format parquet']
        """
        actual = pprint.pformat(
            imvadauddu.get_download_bulk_data_command(components_dict)
        )
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test3(self) -> None:
        """
        Check that the method raises an error when a required parameter is
        missing.
        """
        # Prepare input data.
        components_dict = {
            "end_timestamp": "2021-01-01T00:00:00Z",
            "vendor": "ccxt",
            "exchange_id": "binance",
            "universe": "v8",
            "data_type": "ohlcv",
            "contract_type": "spot",
            "s3_path": "s3://bucket",
            "download_mode": "periodic_daily",
            "downloading_entity": "airflow",
            "action_tag": "downloaded_1min",
            "data_format": "parquet",
        }
        with self.assertRaises(ValueError) as error:
            imvadauddu.get_download_bulk_data_command(components_dict)
        actual = str(error.exception)
        expected = r"""Missing required parameter: start_timestamp"""
        self.assert_equal(actual, expected, fuzzy_match=True)


class Test_get_resample_data_command(hunitest.TestCase):
    def test1(self) -> None:
        """
        Check that the method correctly handles input with all parameters.
        """
        # Prepare input data.
        components_dict = {
            "start_timestamp": "2021-01-01T00:00:00Z",
            "end_timestamp": "2021-01-01T00:01:00Z",
            "src_signature": "periodic_daily.airflow.archived_200ms.parquet.bid_ask.futures.v7_5.ccxt.cryptocom.v1_0_0",
            "dst_signature": "periodic_daily.airflow.resampled_1min.parquet.bid_ask.futures.v7_5.ccxt.cryptocom.v2_0_0",
            "src_s3_path": "s3://bucket",
            "dst_s3_path": "s3://bucket",
            "bid_ask_levels": "1",
            "assert_all_resampled": "",
        }
        expected = r"""
        ['/app/amp/im_v2/common/data/transform/resample_daily_bid_ask_data.py',
        "--start_timestamp '2021-01-01T00:00:00Z'",
        "--end_timestamp '2021-01-01T00:01:00Z'",
        '--src_signature '
        'periodic_daily.airflow.archived_200ms.parquet.bid_ask.futures.v7_5.ccxt.cryptocom.v1_0_0',
        '--dst_signature '
        'periodic_daily.airflow.resampled_1min.parquet.bid_ask.futures.v7_5.ccxt.cryptocom.v2_0_0',
        '--src_s3_path s3://bucket',
        '--dst_s3_path s3://bucket',
        '--bid_ask_levels 1',
        '--assert_all_resampled ']
        """
        actual = pprint.pformat(
            imvadauddu.get_resample_data_command(components_dict)
        )
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test2(self) -> None:
        """
        Check that the method raises an error when a required parameter is
        missing.
        """
        # Prepare input data.
        components_dict = {
            "end_timestamp": "2021-01-01T00:00:00Z",
            "src_signature": "periodic_daily.airflow.archived_200ms.parquet.bid_ask.futures.v7_5.ccxt.cryptocom.v1_0_0",
            "dst_signature": "periodic_daily.airflow.resampled_1min.parquet.bid_ask.futures.v7_5.ccxt.cryptocom.v2_0_0",
            "src_s3_path": "s3://bucket",
            "dst_s3_path": "s3://bucket",
            "bid_ask_levels": "1",
            "assert_all_resampled": "",
        }
        with self.assertRaises(ValueError) as error:
            imvadauddu.get_resample_data_command(components_dict)
        actual = str(error.exception)
        expected = r"""Missing required parameter: start_timestamp"""
        self.assert_equal(actual, expected, fuzzy_match=True)


class Test_get_run_single_dataset_qa_command(hunitest.TestCase):
    def test1(self) -> None:
        """
        Check that the method correctly handles input with all parameters.
        """
        # Prepare input data.
        components_dict = {
            "stage": "preprod",
            "dataset-signature": "realtime.airflow.resampled_1min.postgres.bid_ask.futures.v8_1.binance.binance.v1_0_0",
            "end-timestamp": "2021-01-01T00:00:00Z",
            "start-timestamp": "2021-01-01T00:01:00Z",
            "base-dst-dir": "/mnt/efs/preprod/data_qa/periodic_10min",
            "vendor": "binance",
            "exchange_id": "binance",
            "universe": "v8.1",
            "data_type": "bid_ask",
            "data_format": "parquet",
            "bid-ask-depth": "1",
            "bid-ask-frequency-sec": "60S",
        }
        expected = r"""
        ['mkdir /.dockerenv',
        '&&',
        'invoke run_single_dataset_qa_notebook',
        "--aws-profile 'ck'",
        '--stage preprod',
        '--dataset-signature '
        'realtime.airflow.resampled_1min.postgres.bid_ask.futures.v8_1.binance.binance.v1_0_0',
        "--start-timestamp '2021-01-01T00:01:00Z'",
        "--end-timestamp '2021-01-01T00:00:00Z'",
        '--base-dst-dir /mnt/efs/preprod/data_qa/periodic_10min',
        '--bid-ask-depth 1',
        '--bid-ask-frequency-sec 60S']
        """
        # Test method.
        actual = pprint.pformat(
            imvadauddu.get_run_single_dataset_qa_command(components_dict)
        )
        # Verify output.
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test2(self) -> None:
        """
        Check that the method correctly handles input with all parameters
        except optional.
        """
        # Prepare input data.
        components_dict = {
            "stage": "preprod",
            "dataset-signature": "realtime.airflow.resampled_1min.postgres.bid_ask.futures.v8_1.binance.binance.v1_0_0",
            "end-timestamp": "2021-01-01T00:00:00Z",
            "start-timestamp": "2021-01-01T00:01:00Z",
            "base-dst-dir": "/mnt/efs/preprod/data_qa/periodic_10min",
            "vendor": "binance",
            "exchange_id": "binance",
            "universe": "v8.1",
            "data_type": "bid_ask",
            "data_format": "parquet",
        }
        expected = r"""
        ['mkdir /.dockerenv',
        '&&',
        'invoke run_single_dataset_qa_notebook',
        "--aws-profile 'ck'",
        '--stage preprod',
        '--dataset-signature '
        'realtime.airflow.resampled_1min.postgres.bid_ask.futures.v8_1.binance.binance.v1_0_0',
        "--start-timestamp '2021-01-01T00:01:00Z'",
        "--end-timestamp '2021-01-01T00:00:00Z'",
        '--base-dst-dir /mnt/efs/preprod/data_qa/periodic_10min']
        """
        # Test method.
        actual = pprint.pformat(
            imvadauddu.get_run_single_dataset_qa_command(components_dict)
        )
        # Verify output.
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test3(self) -> None:
        """
        Check that the method raises an error when a required parameter is
        missing.
        """
        # Prepare input data.
        components_dict = {
            "stage": "preprod",
            "end-timestamp": "2021-01-01T00:00:00Z",
            "start-timestamp": "2021-01-01T00:01:00Z",
            "base-dst-dir": "/mnt/efs/preprod/data_qa/periodic_10min",
            "vendor": "binance",
            "exchange_id": "binance",
            "universe": "v8.1",
            "data_type": "bid_ask",
            "data_format": "parquet",
        }
        # Test method.
        with self.assertRaises(ValueError) as error:
            imvadauddu.get_run_single_dataset_qa_command(components_dict)
        actual = str(error.exception)
        expected = r"""Missing required parameter: dataset-signature"""
        # Verify output.
        self.assert_equal(actual, expected, fuzzy_match=True)


class Test_get_resample_websocket_data_command(hunitest.TestCase):
    def test1(self) -> None:
        """
        Check that the method correctly handles input with all parameters.
        """
        # Prepare input data.
        components_dict = {
            "db_stage": "preprod",
            "start_ts": "2021-01-01T00:00:00Z",
            "end_ts": "2021-01-01T00:01:00Z",
            "resample_freq": "1T",
            "dag_signature": "realtime.airflow.downloaded_200ms.postgres.ohlcv.futures.v8.ccxt.binance.v1_0_0",
        }
        expected = r"""
        ['/app/amp/im_v2/common/data/transform/resample_rt_bid_ask_data_periodically.py',
        '--db_stage preprod',
        "--start_ts '2021-01-01T00:00:00Z'",
        "--end_ts '2021-01-01T00:01:00Z'",
        '--resample_freq 1T',
        '--dag_signature '
        'realtime.airflow.downloaded_200ms.postgres.ohlcv.futures.v8.ccxt.binance.v1_0_0']
        """
        actual = pprint.pformat(
            imvadauddu.get_resample_websocket_data_command(components_dict)
        )
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test2(self) -> None:
        """
        Check that the method raises an error when a required parameter is
        missing.
        """
        # Prepare input data.
        components_dict = {
            "db_stage": "preprod",
            "start_ts": "2021-01-01T00:00:00Z",
            "resample_freq": "1T",
            "dag_signature": "realtime.airflow.downloaded_200ms.postgres.ohlcv.futures.v8.ccxt.binance.v1_0_0",
        }
        with self.assertRaises(ValueError) as error:
            imvadauddu.get_resample_websocket_data_command(components_dict)
        actual = str(error.exception)
        expected = r"""Missing required parameter: end_ts"""
        self.assert_equal(actual, expected, fuzzy_match=True)
