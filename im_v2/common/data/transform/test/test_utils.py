import pandas as pd

import helpers.hunit_test as hunitest
import im_v2.common.data.transform.utils as imvcdtrut


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


class TestPartitionDataset(hunitest.TestCase):
    def test_partition_dataset(self) -> None:
        """
        Verify regular creation of partition datasets with existing columns.
        """
        test_dir = self.get_scratch_space()
        test_data = {
            "dummy_value_1": [1, 2, 3],
            "dummy_value_2": ["A", "B", "C"],
            "dummy-value_3": [0, 0, 0],
        }
        dummy_df = pd.DataFrame(data=test_data)
        partition_cols = ["dummy_value_1", "dummy_value_2"]
        imvcdtrut.partition_dataset(dummy_df, partition_cols, test_dir)
        include_file_content = True
        dir_signature = hunitest.get_dir_signature(test_dir, include_file_content)
        self.check_string(dir_signature, purify_text=True)

    def test_partition_dataset_wrong_column(self) -> None:
        """
        Assert that wrong columns are detected on before partitioning.
        """
        test_dir = self.get_scratch_space()
        test_data = {
            "dummy_value_1": [1, 2, 3],
            "dummy_value_2": ["A", "B", "C"],
            "dummy-value_3": [0, 0, 0],
        }
        dummy_df = pd.DataFrame(data=test_data)
        partition_cols = ["void_column", "dummy_value_2"]
        with self.assertRaises(AssertionError):
            imvcdtrut.partition_dataset(dummy_df, partition_cols, test_dir)


class TestConvertTimestampColumn(hunitest.TestCase):
    def test_integer_datetime(self) -> None:
        """
        Verify that integer datetime is converted correctly.
        """
        test_data = pd.Series([1638756800000, 1639656800000, 1648656800000])
        actual = imvcdtrut.convert_timestamp_column(test_data)
        actual = str(actual)
        self.check_string(actual)

    def test_string_datetime(self) -> None:
        """
        Verify that string datetime is converted correctly.
        """
        test_data = pd.Series(["2021-01-12", "2021-02-14", "2010-12-11"])
        actual = imvcdtrut.convert_timestamp_column(test_data)
        actual = str(actual)
        self.check_string(actual)

    def test_incorrect_datetime(self) -> None:
        """
        Assert that incorrect types are not converted.
        """
        test_data = pd.Series([37.9, 88.11, 14.0])
        with self.assertRaises(ValueError):
            imvcdtrut.convert_timestamp_column(test_data)


class TestReindexOnDatetime(hunitest.TestCase):
    def test_reindex_on_datetime_milliseconds(self) -> None:
        """
        Verify datetime index creation when timestamp is in milliseconds.
        """
        dummy_df = _get_dummy_df_with_timestamp()
        reindexed_dummy_df = imvcdtrut.reindex_on_datetime(
            dummy_df, "dummy_timestamp"
        )
        reindexed_txt_df = hunitest.convert_df_to_json_string(
            reindexed_dummy_df, n_tail=None
        )
        self.check_string(reindexed_txt_df)

    def test_reindex_on_datetime_seconds(self) -> None:
        """
        Verify datetime index creation when timestamp is in seconds.
        """
        dummy_df = _get_dummy_df_with_timestamp(unit="s")
        reindexed_dummy_df = imvcdtrut.reindex_on_datetime(
            dummy_df, "dummy_timestamp", unit="s"
        )
        reindexed_txt_df = hunitest.convert_df_to_json_string(
            reindexed_dummy_df, n_tail=None
        )
        self.check_string(reindexed_txt_df)

    def test_reindex_on_datetime_wrong_column(self) -> None:
        """
        Assert that wrong column is detected before reindexing.
        """
        dummy_df = _get_dummy_df_with_timestamp()
        with self.assertRaises(AssertionError):
            imvcdtrut.reindex_on_datetime(dummy_df, "void_column")

    def test_reindex_on_datetime_index_already_present(self) -> None:
        """
        Assert that reindexing is not done on already reindexed dataframe.
        """
        dummy_df = _get_dummy_df_with_timestamp()
        reindexed_dummy_df = imvcdtrut.reindex_on_datetime(
            dummy_df, "dummy_timestamp"
        )
        with self.assertRaises(AssertionError):
            imvcdtrut.reindex_on_datetime(reindexed_dummy_df, "dummy")


class TestAddDatePartitionCols(hunitest.TestCase):
    def test_add_date_partition_cols(self) -> None:
        """
        Verify that generic date column is present in dataframe.
        """
        dummy_df = _get_dummy_df_with_timestamp()
        reindexed_dummy_df = imvcdtrut.reindex_on_datetime(
            dummy_df, "dummy_timestamp"
        )
        imvcdtrut.add_date_partition_cols(reindexed_dummy_df)
        reindexed_txt_df = hunitest.convert_df_to_json_string(
            reindexed_dummy_df, n_tail=None
        )
        self.check_string(reindexed_txt_df)

    def test_add_date_partition_cols_year(self) -> None:
        """
        Verify that year column is present in dataframe.
        """
        dummy_df = _get_dummy_df_with_timestamp()
        reindexed_dummy_df = imvcdtrut.reindex_on_datetime(
            dummy_df, "dummy_timestamp"
        )
        imvcdtrut.add_date_partition_cols(reindexed_dummy_df, "year")
        reindexed_txt_df = hunitest.convert_df_to_json_string(
            reindexed_dummy_df, n_tail=None
        )
        self.check_string(reindexed_txt_df)

    def test_add_date_partition_cols_month(self) -> None:
        """
        Verify that year and month columns are present in dataframe.
        """
        dummy_df = _get_dummy_df_with_timestamp()
        reindexed_dummy_df = imvcdtrut.reindex_on_datetime(
            dummy_df, "dummy_timestamp"
        )
        imvcdtrut.add_date_partition_cols(reindexed_dummy_df, "month")
        reindexed_txt_df = hunitest.convert_df_to_json_string(
            reindexed_dummy_df, n_tail=None
        )
        self.check_string(reindexed_txt_df)

    def test_add_date_partition_cols_day(self) -> None:
        """
        Verify that year, month and day columns are present in dataframe.
        """
        dummy_df = _get_dummy_df_with_timestamp()
        reindexed_dummy_df = imvcdtrut.reindex_on_datetime(
            dummy_df, "dummy_timestamp"
        )
        imvcdtrut.add_date_partition_cols(reindexed_dummy_df, "day")
        reindexed_txt_df = hunitest.convert_df_to_json_string(
            reindexed_dummy_df, n_tail=None
        )
        self.check_string(reindexed_txt_df)

    def test_add_date_partition_cols_wrong_partition_mode(self) -> None:
        """
        Assert that proper partition mode is used.
        """
        dummy_df = _get_dummy_df_with_timestamp()
        reindexed_dummy_df = imvcdtrut.reindex_on_datetime(
            dummy_df, "dummy_timestamp"
        )
        with self.assertRaises(AssertionError):
            imvcdtrut.add_date_partition_cols(reindexed_dummy_df, "void_mode")
