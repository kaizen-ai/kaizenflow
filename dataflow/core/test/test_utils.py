import datetime
import logging
from typing import Tuple

import pandas as pd

import dataflow.core.utils as dtfcorutil
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)


class Test_get_df_info_as_string(hunitest.TestCase):
    def test1(self) -> None:
        df = pd.DataFrame({"col_1": [1, 2], "col_2": [3, 4]})
        info = dtfcorutil.get_df_info_as_string(df, exclude_memory_usage=False)
        self.check_string(info)

    def test2(self) -> None:
        df = pd.DataFrame({"col_1": [1, 2], "col_2": [3, 4]})
        info = dtfcorutil.get_df_info_as_string(df)
        self.check_string(info)


class Test_get_DagBuilder_name_from_string(hunitest.TestCase):
    """
    Test that the function returns a correct DAG builder name.
    """

    def test1(self) -> None:
        dag_builder_ctor_as_str = (
            "dataflow_orange.pipelines.C1.C1b_pipeline.C1b_DagBuilder"
        )
        act = dtfcorutil.get_DagBuilder_name_from_string(dag_builder_ctor_as_str)
        exp = "C1b"
        self.assert_equal(act, exp)

    def test2(self) -> None:
        dag_builder_ctor_as_str = (
            "dataflow_orange.pipelines.C3.C3a_pipeline_tmp.C3a_DagBuilder_tmp"
        )
        act = dtfcorutil.get_DagBuilder_name_from_string(dag_builder_ctor_as_str)
        exp = "C3a"
        self.assert_equal(act, exp)

    def test3(self) -> None:
        dag_builder_ctor_as_str = (
            "dataflow_lemonade.pipelines.C5.C5b_pipeline.C5b_DagBuilder"
        )
        act = dtfcorutil.get_DagBuilder_name_from_string(dag_builder_ctor_as_str)
        exp = "C5b"
        self.assert_equal(act, exp)


class Test_convert_to_multiindex(hunitest.TestCase):
    @staticmethod
    def get_test_data_multiple_asset() -> Tuple[pd.DataFrame, pd.MultiIndex]:
        """
        Function that return dummy dataframe with multiple asset id and
        Multiindex.
        """
        data = {
            "id": [13684, 17085, 13684, 17085, 13684],
            "close": [None, None, None, None, None],
            "volume": [0, 0, 0, 0, 0],
        }
        index = pd.to_datetime(
            ["2022-01-04 09:01:00-05:00"] * 2
            + ["2022-01-04 09:02:00-05:00"] * 2
            + ["2022-01-04 09:03:00-05:00"]
        )
        df = pd.DataFrame(data, index=index)
        expected_df_columns = pd.MultiIndex.from_product(
            [["close", "volume"], [13684, 17085]], names=[None, "id"]
        )
        return df, expected_df_columns

    @staticmethod
    def get_test_data_single_asset() -> Tuple[pd.DataFrame, pd.MultiIndex]:
        """
        Function that return dummy dataframe with single asset id and
        Multiindex.
        """
        data = {
            "id": [13684, 13684, 13684],
            "close": [None, None, None],
            "volume": [0, 0, 0],
        }
        index = pd.to_datetime(
            [
                "2022-01-04 09:01:00-05:00",
                "2022-01-04 09:02:00-05:00",
                "2022-01-04 09:03:00-05:00",
            ]
        )
        df = pd.DataFrame(data, index=index)
        expected_df_columns = pd.MultiIndex.from_product(
            [["close", "volume"], [13684]], names=[None, "id"]
        )
        return df, expected_df_columns

    def assert_multiindex_columns_equal(
        self, expected_df_columns: pd.MultiIndex, actual_df: pd.DataFrame
    ) -> None:
        """
        Compare the list of columns of actual df and and expected df.
        """
        expected_df_columns = str(expected_df_columns.to_list())
        actual_df_columns = str(actual_df.columns.to_list())
        self.assert_equal(expected_df_columns, actual_df_columns)

    def test1(self) -> None:
        """
        Test that a function transforms the DataFrame correctly.
        """
        # Prepare input and expected output.
        df, expected_df_columns = self.get_test_data_multiple_asset()
        asset_id_col = "id"
        # Prepare the actual output by running function.
        actual_df = dtfcorutil.convert_to_multiindex(df, asset_id_col)
        # Compare the result.
        self.assert_multiindex_columns_equal(expected_df_columns, actual_df)

    def test2(self) -> None:
        """
        Test that a function handles a DataFrame with duplicate rows correctly.
        """
        # Prepare input and expected output.
        df, expected_df_columns = self.get_test_data_multiple_asset()
        asset_id_col = "id"
        df = pd.concat([df, df.head(1)])
        # Prepare the actual output by running function.
        actual_df = dtfcorutil.convert_to_multiindex(df, asset_id_col)
        # Compare the result.
        self.assert_multiindex_columns_equal(expected_df_columns, actual_df)

    def test3(self) -> None:
        """
        Test that a function handles an empty dataframe correctly.
        """
        # Creating empty data frame for input.
        df = pd.DataFrame(columns=["id", "close", "volume"])
        asset_id_col = "id"
        with self.assertRaises(AssertionError) as context:
            dtfcorutil.convert_to_multiindex(df, asset_id_col)
        actual_error_message = str(context.exception)
        expected_error_message = r"""
        ################################################################################
        * Failed assertion *
        1 <= 0
        ################################################################################
        """
        self.assert_equal(
            actual_error_message, expected_error_message, fuzzy_match=True
        )

    def test4(self) -> None:
        """
        Test that a function handles a DataFrame with only one asset correctly.
        """
        # Prepare input and expected output.
        df, expected_df_columns = self.get_test_data_single_asset()
        asset_id_col = "id"
        # Prepare the actual output by running function.
        actual_df = dtfcorutil.convert_to_multiindex(df, asset_id_col)
        # Compare the result.
        self.assert_multiindex_columns_equal(expected_df_columns, actual_df)

    def test5(self) -> None:
        """
        Test that a function handles the case where the asset_id_col doesn't
        exist.
        """
        # Prepare input and expected output.
        df = self.get_test_data_single_asset()
        asset_id_col = "nonexistent_column"
        with self.assertRaises(AssertionError) as context:
            dtfcorutil.convert_to_multiindex(df, asset_id_col)
        # Get the actual error message
        actual_error_message = str(context.exception)
        expected_error_message = r"""
        ################################################################################
        * Failed assertion *
        Instance of '(                              id close  volume
        2022-01-04 09:01:00-05:00  13684  None       0
        2022-01-04 09:02:00-05:00  13684  None       0
        2022-01-04 09:03:00-05:00  13684  None       0, MultiIndex([( 'close', 13684),
                                ('volume', 13684)],
                                                      names=[None, 'id']))' is '<class 'tuple'>' instead of '<class 'pandas.core.frame.DataFrame'>'
        ################################################################################
        """
        self.assert_equal(
            actual_error_message, expected_error_message, fuzzy_match=True
        )

    def test6(self) -> None:
        """
        Test that a function handles the case where there is no index in the
        dataframe.
        """
        # Prepare input and expected output.
        df, expected_df_columns = self.get_test_data_single_asset()
        asset_id_col = "id"
        df = df.reset_index(drop=True)
        # Prepare the actual output by running function.
        actual_df = dtfcorutil.convert_to_multiindex(df, asset_id_col)
        # Compare the result.
        self.assert_multiindex_columns_equal(expected_df_columns, actual_df)


class TestFindMinMaxTimestampsFromIntervals(hunitest.TestCase):
    @staticmethod
    def get_data(method_name: str) -> Tuple:
        COMMON_YEAR = 2022
        COMMON_MONTH = 1
        COMMON_DAY = 1
        COMMON_HOUR = 10
        COMMON_TZ = "UTC"
        COMMON_DATETIME_START = datetime.datetime(
            COMMON_YEAR, COMMON_MONTH, COMMON_DAY, COMMON_HOUR, 0, 0
        )
        COMMON_DATETIME_END = datetime.datetime(
            COMMON_YEAR, COMMON_MONTH, COMMON_DAY, COMMON_HOUR + 1, 0, 0
        )
        COMMON_TIMESTAMP_START = pd.Timestamp(
            year=COMMON_YEAR,
            month=COMMON_MONTH,
            day=COMMON_DAY,
            hour=COMMON_HOUR,
            tz=COMMON_TZ,
        )
        COMMON_TIMESTAMP_END = pd.Timestamp(
            year=COMMON_YEAR,
            month=COMMON_MONTH,
            day=COMMON_DAY,
            hour=COMMON_HOUR + 1,
            tz=COMMON_TZ,
        )

        if method_name == "empty_intervals":
            return (None, (None, None))
        elif method_name == "single_interval_with_none_endpoints":
            return ([(None, None)], (None, None))
        elif method_name == "single_interval_with_datetime_endpoints":
            return (
                [(COMMON_DATETIME_START, COMMON_DATETIME_END)],
                (COMMON_DATETIME_START, COMMON_DATETIME_END),
            )
        elif method_name == "single_interval_with_same_datetime_endpoints":
            return (
                [(COMMON_DATETIME_START, COMMON_DATETIME_START)],
                (COMMON_DATETIME_START, COMMON_DATETIME_START),
            )
        elif method_name == "single_interval_with_timestamp_endpoints":
            return (
                [(COMMON_TIMESTAMP_START, COMMON_TIMESTAMP_END)],
                (COMMON_TIMESTAMP_START, COMMON_TIMESTAMP_END),
            )
        elif method_name == "single_interval_with_same_timestamp_endpoints":
            return (
                [(COMMON_TIMESTAMP_START, COMMON_TIMESTAMP_START)],
                (COMMON_TIMESTAMP_START, COMMON_TIMESTAMP_START),
            )
        elif method_name == "multiple_intervals_with_mixed_endpoints":
            return (
                [(COMMON_DATETIME_START, None), (None, COMMON_TIMESTAMP_END)],
                (None, None),
            )
        elif method_name == "multiple_intervals_with_datetime_endpoints":
            interval1_start = COMMON_DATETIME_START
            interval1_end = COMMON_DATETIME_END
            interval2_start = interval1_start - datetime.timedelta(hours=4)
            interval2_end = interval1_end - datetime.timedelta(hours=2)
            intervals = [
                (interval1_start, interval1_end),
                (interval2_start, interval2_end),
            ]
            overall_start = interval2_start
            overall_end = interval1_end
            return (intervals, (overall_start, overall_end))
        elif method_name == "multiple_intervals_with_timestamp_endpoints":
            interval1_start = COMMON_TIMESTAMP_START
            interval1_end = COMMON_TIMESTAMP_END
            interval2_start = interval1_start.tz_convert("EST")
            interval2_end = interval1_end.tz_convert("EST")
            intervals = [
                (interval1_start, interval1_end),
                (interval2_start, interval2_end),
            ]
            overall_start = interval1_start
            overall_end = interval2_end
            return (intervals, (overall_start, overall_end))
        else:
            raise ValueError(f"Invalid method_name: {method_name}")

    def helper(self, intervals, expected_min_max):
        actual_min_max = dtfcorutil.find_min_max_timestamps_from_intervals(
            intervals
        )
        self.assertEqual(expected_min_max, actual_min_max)

    def test1(self) -> None:
        """
        Test case when intervals is None.
        """
        intervals, expected_min_max = self.get_data("empty_intervals")
        self.helper(intervals, expected_min_max)

    def test2(self) -> None:
        """
        Test case with a single interval where both endpoints are None.
        """
        intervals, expected_min_max = self.get_data(
            "single_interval_with_none_endpoints"
        )
        self.helper(intervals, expected_min_max)

    def test3(self) -> None:
        """
        Test case with a single interval with datetime.datetime objects as
        endpoints.
        """
        intervals, expected_min_max = self.get_data(
            "single_interval_with_datetime_endpoints"
        )
        self.helper(intervals, expected_min_max)

    def test4(self) -> None:
        """
        Test case with a single interval with same datetime.datetime objects as
        endpoints.
        """
        intervals, expected_min_max = self.get_data(
            "single_interval_with_same_datetime_endpoints"
        )
        self.helper(intervals, expected_min_max)

    def test5(self) -> None:
        """
        Test case with a single interval with pd.Timestamp objects as
        endpoints.
        """
        intervals, expected_min_max = self.get_data(
            "single_interval_with_timestamp_endpoints"
        )
        self.helper(intervals, expected_min_max)

    def test6(self) -> None:
        """
        Test case with a single interval with same pd.Timestamp objects as
        endpoints.
        """
        intervals, expected_min_max = self.get_data(
            "single_interval_with_same_timestamp_endpoints"
        )
        self.helper(intervals, expected_min_max)

    def test7(self) -> None:
        """
        Test case with multiple intervals with a mix of datetime.datetime,
        pd.Timestamp objects, and None as endpoints.
        """
        intervals, expected_min_max = self.get_data(
            "multiple_intervals_with_mixed_endpoints"
        )
        self.helper(intervals, expected_min_max)

    def test8(self) -> None:
        """
        Test case with multiple intervals with datetime.datetime objects as
        endpoints.
        """
        intervals, expected_min_max = self.get_data(
            "multiple_intervals_with_datetime_endpoints"
        )
        self.helper(intervals, expected_min_max)

    def test9(self) -> None:
        """
        Test case with multiple intervals with pd.Timestamp objects as
        endpoints.
        """
        intervals, expected_min_max = self.get_data(
            "multiple_intervals_with_timestamp_endpoints"
        )
        self.helper(intervals, expected_min_max)
