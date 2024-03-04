import logging

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
    def get_test_data() -> pd.DataFrame:
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
        return df

    def test1(self) -> None:
        """
        Test that the function transforms the DataFrame correctly.
        """
        df = self.get_test_data()
        asset_id_col = "id"
        result_df = dtfcorutil.convert_to_multiindex(df, asset_id_col)

        expected_columns = pd.MultiIndex.from_product(
            [["close", "volume"], [13684, 17085]], names=[None, "id"]
        )
        self.assert_equal(
            str(expected_columns.to_list()), str(result_df.columns.to_list())
        )

    def test2(self) -> None:
        """
        Test that the function handles a DataFrame with duplicate rows
        correctly.
        """
        df = self.get_test_data()
        asset_id_col = "id"

        df = pd.concat([df, df.head(1)])
        result_df = dtfcorutil.convert_to_multiindex(df, asset_id_col)
        expected_columns = pd.MultiIndex.from_product(
            [["close", "volume"], [13684, 17085]], names=[None, "id"]
        )
        self.assert_equal(
            str(expected_columns.to_list()), str(result_df.columns.to_list())
        )
