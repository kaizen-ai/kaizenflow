import pandas as pd
import unittest.mock as umock
import helpers.hunit_test as hunitest
import im_v2.common.db.db_utils as imvcddbut
import im_v2.common.data.client.im_raw_data_client as imvcdcimrdc


class TestLogExperimentData(imvcddbut.TestImDbHelper):

    def helper(self) -> pd.DataFrame:
        """
        Fetch bid/ask data for testing.
        """
        self.start_timestamp = pd.Timestamp("2024-01-01 01:10:00+00:00")
        test_data = {
            "currency_pair": ["BTC_USDT"] * 10,
            "bid_size": list(range(50,150, 10)),
            "bid_price": list(range(100,600, 100)) * 2,
            "ask_size": list(range(10,110, 10)),
            "ask_price": list(range(150, 650, 50)),
            "level": [1, 2] * 5,
            "timestamp": pd.date_range(self.start_timestamp, periods=10, freq='s')
        }
        test_df = pd.DataFrame(test_data)
        return test_df
    
    @umock.patch.object(
        imvcdcimrdc.imvcddbut, "load_db_data"
    )
    def test_log_experiment_data(self, mock_load_db_data: umock.MagicMock):
        """
        Verify the saved data after reloading from database.
        """
        db_stage = "local"
        data_vendor = "CCXT"
        universe = "v7.4"
        mock_load_db_data.return_value = self.helper()
        # Call function to load saved data from db
        bid_ask_raw_data_reader = imvcdcimrdc.get_bid_ask_realtime_raw_data_reader(
            db_stage, data_vendor, universe
        )
        bid_ask_data = bid_ask_raw_data_reader.load_db_table(
            self.start_timestamp,
            self.start_timestamp + pd.Timedelta(seconds=10),
            bid_ask_levels=[1],
            deduplicate=True,
            subset=[
                "timestamp",
                "currency_pair",
                "bid_price",
                "bid_size",
                "ask_price",
                "ask_size",
                "level",
            ],
        )
        # Define expected values
        expected_data = r""""""
        self.assert_equal(bid_ask_data, expected_data)