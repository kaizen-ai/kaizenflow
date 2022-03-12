import pandas as pd

import helpers.hunit_test as hunitest
import im_v2.common.data.transform.transform_utils as imvcdttrut


class TestConvertTimestampColumn(hunitest.TestCase):
    def test_integer_datetime(self) -> None:
        """
        Verify that integer datetime is converted correctly.
        """
        # Prepare inputs.
        test_data = pd.Series([1638756800000, 1639656800000, 1648656800000])
        # Run.
        actual = imvcdttrut.convert_timestamp_column(test_data)
        # Check output.
        actual = str(actual)
        expected = """
        0   2021-12-06 02:13:20+00:00
        1   2021-12-16 12:13:20+00:00
        2   2022-03-30 16:13:20+00:00
        dtype: datetime64[ns, UTC]
        """
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test_string_datetime(self) -> None:
        """
        Verify that string datetime is converted correctly.
        """
        # Prepare inputs.
        test_data = pd.Series(["2021-01-12", "2021-02-14", "2010-12-11"])
        # Run.
        actual = imvcdttrut.convert_timestamp_column(test_data)
        # Check output.
        actual = str(actual)
        expected = """
        0   2021-01-12
        1   2021-02-14
        2   2010-12-11
        dtype: datetime64[ns]
        """
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test_incorrect_datetime(self) -> None:
        """
        Assert that incorrect types are not converted.
        """
        test_data = pd.Series([37.9, 88.11, 14.0])
        with self.assertRaises(ValueError) as fail:
            imvcdttrut.convert_timestamp_column(test_data)
        actual = str(fail.exception)
        expected = (
            "Incorrect data format. Datetime column should be of int or str dtype"
        )
        self.assert_equal(actual, expected)


# #############################################################################


class TestReindexOnDatetime(hunitest.TestCase):
    def test_reindex_on_datetime_milliseconds(self) -> None:
        """
        Verify datetime index creation when timestamp is in milliseconds.
        """
        # Prepare inputs.
        dummy_df = self._get_dummy_df_with_timestamp()
        # Run.
        reindexed_dummy_df = imvcdttrut.reindex_on_datetime(
            dummy_df, "dummy_timestamp"
        )
        # Check output.
        actual = str(reindexed_dummy_df)
        expected = (
            "                           dummy_value  dummy_timestamp\n"
            "dummy_timestamp                                        \n"
            "2021-12-04 19:40:00+00:00            1    1638646800000\n"
            "2021-12-04 19:41:00+00:00            2    1638646860000\n"
            "2021-12-04 19:42:40+00:00            3    1638646960000"
        )
        self.assert_equal(actual, expected)

    def test_reindex_on_datetime_seconds(self) -> None:
        """
        Verify datetime index creation when timestamp is in seconds.
        """
        # Prepare inputs.
        dummy_df = self._get_dummy_df_with_timestamp(unit="s")
        # Run.
        reindexed_dummy_df = imvcdttrut.reindex_on_datetime(
            dummy_df, "dummy_timestamp", unit="s"
        )
        actual = str(reindexed_dummy_df)
        expected = (
            "                           dummy_value  dummy_timestamp\n"
            "dummy_timestamp                                        \n"
            "2021-12-04 19:40:00+00:00            1       1638646800\n"
            "2021-12-04 19:41:00+00:00            2       1638646860\n"
            "2021-12-04 19:42:40+00:00            3       1638646960"
        )
        self.assert_equal(actual, expected)

    def test_reindex_on_datetime_wrong_column(self) -> None:
        """
        Assert that wrong column is detected before reindexing.
        """
        dummy_df = self._get_dummy_df_with_timestamp()
        with self.assertRaises(AssertionError) as fail:
            imvcdttrut.reindex_on_datetime(dummy_df, "void_column")
        actual = str(fail.exception)
        expected = """
        * Failed assertion *
        'void_column' in 'Index(['dummy_value', 'dummy_timestamp'], dtype='object')'
        """
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test_reindex_on_datetime_index_already_present(self) -> None:
        """
        Assert that reindexing is not done on already reindexed dataframe.
        """
        dummy_df = self._get_dummy_df_with_timestamp()
        reindexed_dummy_df = imvcdttrut.reindex_on_datetime(
            dummy_df, "dummy_timestamp"
        )
        with self.assertRaises(AssertionError) as fail:
            imvcdttrut.reindex_on_datetime(reindexed_dummy_df, "dummy")
        actual = str(fail.exception)
        expected = """
        * Failed assertion *
        'dummy' in 'Index(['dummy_value', 'dummy_timestamp'], dtype='object')'
        """
        self.assert_equal(actual, expected, fuzzy_match=True)

    @staticmethod
    def _get_dummy_df_with_timestamp(unit: str = "ms") -> pd.DataFrame:
        datetime_column_name = "dummy_timestamp"
        test_data = {
            "dummy_value": [1, 2, 3],
            datetime_column_name: [1638646800000, 1638646860000, 1638646960000],
        }
        if unit == "s":
            test_data[datetime_column_name] = [
                timestamp // 1000 for timestamp in test_data[datetime_column_name]
            ]
        return pd.DataFrame(data=test_data)