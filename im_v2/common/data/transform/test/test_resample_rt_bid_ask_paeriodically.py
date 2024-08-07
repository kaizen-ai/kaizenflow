import data_schema.dataset_schema_utils as dsdascut
import helpers.hunit_test as hunitest
import im_v2.common.data.transform.resample_rt_bid_ask_data_periodically as imvcdtrrbadp


class Test__build_table_names_from_signature(hunitest.TestCase):
    dataset_schema = dsdascut.get_dataset_schema()

    def test1(self) -> None:
        """
        Check that the function correctly handles params with 1-minute
        frequency.
        """
        # Prepare input.
        freq = "1T"
        signature = "realtime.airflow.downloaded_200ms.postgres.bid_ask.futures.v8_1.ccxt.binance.v1_0_0"
        expected_src_table = "ccxt_bid_ask_futures_raw"
        expected_dst_table = "ccxt_bid_ask_futures_resampled_1min"
        # Test method.
        (
            actual_src_table,
            actual_dst_table,
        ) = imvcdtrrbadp._build_table_name_from_signature(
            signature, self.dataset_schema, freq
        )
        # Verify outputs.
        self.assert_equal(actual_src_table, expected_src_table)
        self.assert_equal(actual_dst_table, expected_dst_table)

    def test2(self) -> None:
        """
        Check that the function correctly handles params with 10-second
        frequency.
        """
        # Prepare input.
        freq = "10S"
        signature = "realtime.airflow.downloaded_200ms.postgres.bid_ask.futures.v8_1.ccxt.binance.v1_0_0"
        expected_src_table = "ccxt_bid_ask_futures_raw"
        expected_dst_table = "ccxt_bid_ask_futures_resampled_10sec"
        # Test method.
        (
            actual_src_table,
            actual_dst_table,
        ) = imvcdtrrbadp._build_table_name_from_signature(
            signature, self.dataset_schema, freq
        )
        # Verify outputs.
        self.assert_equal(actual_src_table, expected_src_table)
        self.assert_equal(actual_dst_table, expected_dst_table)
