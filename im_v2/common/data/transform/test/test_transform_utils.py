import pandas as pd

import helpers.hpandas as hpandas
import helpers.hunit_test as hunitest
import im_v2.common.data.transform.transform_utils as imvcdttrut


def _get_dummy_df_with_timestamp(
    unit: str = "ms", datetime_col_name: str = "dummy_timestamp"
) -> pd.DataFrame:
    test_data = {
        "dummy_value": [1, 2, 3],
        datetime_col_name: [1638646800000, 1638646860000, 1638646960000],
    }
    if unit == "s":
        test_data[datetime_col_name] = [
            timestamp // 1000 for timestamp in test_data[datetime_col_name]
        ]
    return pd.DataFrame(data=test_data)


# #############################################################################


class TestPartitionDataset(hunitest.TestCase):
    @staticmethod
    def get_test_data1() -> pd.DataFrame:
        test_data = {
            "dummy_value_1": [1, 2, 3],
            "dummy_value_2": ["A", "B", "C"],
            "dummy_value_3": [0, 0, 0],
        }
        df = pd.DataFrame(data=test_data)
        return df

    def test_get_test_data1(self) -> None:
        test_data = self.get_test_data1()
        act = hpandas.df_to_str(test_data)
        exp = r"""   
           dummy_value_1 dummy_value_2  dummy_value_3
        0              1             A              0
        1              2             B              0
        2              3             C              0"""
        self.assert_equal(act, exp, fuzzy_match=True)

    def test_partition_dataset(self) -> None:
        """
        Test partitioned Parquet datasets with existing columns.
        """
        # Prepare inputs.
        test_dir = self.get_scratch_space()
        df = self.get_test_data1()
        # Run.
        partition_cols = ["dummy_value_1", "dummy_value_2"]
        imvcdttrut.partition_dataset(df, partition_cols, test_dir)
        # Check output.
        include_file_content = False
        remove_dir_name = True
        dir_signature = hunitest.get_dir_signature(
            test_dir, include_file_content, remove_dir_name=remove_dir_name
        )
        exp = r"""
        # Dir structure
        .
        dummy_value_1=1
        dummy_value_1=1/dummy_value_2=A
        dummy_value_1=1/dummy_value_2=A/data.parquet
        dummy_value_1=2
        dummy_value_1=2/dummy_value_2=B
        dummy_value_1=2/dummy_value_2=B/data.parquet
        dummy_value_1=3
        dummy_value_1=3/dummy_value_2=C
        dummy_value_1=3/dummy_value_2=C/data.parquet"""
        self.assert_equal(dir_signature, exp, purify_text=True, fuzzy_match=True)
        #
        include_file_content = True
        dir_signature = hunitest.get_dir_signature(
            test_dir, include_file_content, remove_dir_name=remove_dir_name
        )
        self.check_string(dir_signature, purify_text=True, fuzzy_match=True)

    def test_partition_dataset_wrong_column(self) -> None:
        """
        Assert that wrong columns are detected before partitioning.
        """
        # Prepare inputs.
        test_dir = self.get_scratch_space()
        df = self.get_test_data1()
        # Run.
        partition_cols = ["void_column", "dummy_value_2"]
        # Check output.
        with self.assertRaises(AssertionError) as cm:
            imvcdttrut.partition_dataset(df, partition_cols, test_dir)
        act = str(cm.exception)
        exp = r"""
        * Failed assertion *
        val1=['dummy_value_2', 'void_column']
        issubset
        val2=['dummy_value_1', 'dummy_value_2', 'dummy_value_3']
        val1 - val2=['void_column']
        """
        self.assert_equal(act, exp, fuzzy_match=True)


# #############################################################################


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
        dummy_df = _get_dummy_df_with_timestamp()
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
        dummy_df = _get_dummy_df_with_timestamp(unit="s")
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
        dummy_df = _get_dummy_df_with_timestamp()
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
        dummy_df = _get_dummy_df_with_timestamp()
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


# #############################################################################


class TestAddDatePartitionCols(hunitest.TestCase):
    def add_date_partition_columns_helper(
        self, partition_mode: str, expected: str
    ) -> None:
        # Prepare inputs.
        df = _get_dummy_df_with_timestamp()
        reindexed_df = imvcdttrut.reindex_on_datetime(df, "dummy_timestamp")
        # Run.
        imvcdttrut.add_date_partition_cols(reindexed_df, partition_mode)
        # Check output.
        actual = hpandas.df_to_str(reindexed_df)
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test_add_date_partition_cols1(self) -> None:
        partition_mode = "by_date"
        expected = r"""
                                   dummy_value  dummy_timestamp      date
        dummy_timestamp
        2021-12-04 19:40:00+00:00            1    1638646800000  20211204
        2021-12-04 19:41:00+00:00            2    1638646860000  20211204
        2021-12-04 19:42:40+00:00            3    1638646960000  20211204"""
        self.add_date_partition_columns_helper(partition_mode, expected)

    def test_add_date_partition_cols2(self) -> None:
        partition_mode = "by_year"
        expected = r"""
                                   dummy_value  dummy_timestamp  year
        dummy_timestamp
        2021-12-04 19:40:00+00:00            1    1638646800000  2021
        2021-12-04 19:41:00+00:00            2    1638646860000  2021
        2021-12-04 19:42:40+00:00            3    1638646960000  2021"""
        self.add_date_partition_columns_helper(partition_mode, expected)

    def test_add_date_partition_cols3(self) -> None:
        partition_mode = "by_year_month_day"
        expected = r"""
                           dummy_value  dummy_timestamp  year  month        date
dummy_timestamp
2021-12-04 19:40:00+00:00            1    1638646800000  2021     12  2021-12-04
2021-12-04 19:41:00+00:00            2    1638646860000  2021     12  2021-12-04
2021-12-04 19:42:40+00:00            3    1638646960000  2021     12  2021-12-04"""
        self.add_date_partition_columns_helper(partition_mode, expected)

    def test_add_date_partition_cols4(self) -> None:
        partition_mode = "by_year_week"
        expected = r"""
                                   dummy_value  dummy_timestamp  year  weekofyear
        dummy_timestamp
        2021-12-04 19:40:00+00:00            1    1638646800000  2021          48
        2021-12-04 19:41:00+00:00            2    1638646860000  2021          48
        2021-12-04 19:42:40+00:00            3    1638646960000  2021          48"""
        self.add_date_partition_columns_helper(partition_mode, expected)
