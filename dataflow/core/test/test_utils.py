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


class Test_get_DagBuilder_name_from_string_pointer(hunitest.TestCase):
    """
    Test that `get_DagBuilder_name_from_string_pointer()` returns a correct DAG
    builder name.
    """
    def test1(self) -> None:
        dag_builder_ctor_as_str = (
            "dataflow_orange.pipelines.C1.C1b_pipeline.C1b_DagBuilder"
        )
        act = dtfcorutil.get_DagBuilder_name_from_string_pointer(
            dag_builder_ctor_as_str
        )
        exp = "C1b"
        self.assert_equal(act, exp)

    def test2(self) -> None:
        dag_builder_ctor_as_str = (
            "dataflow_orange.pipelines.C3.C3a_pipeline_tmp.C3a_DagBuilder_tmp"
        )
        act = dtfcorutil.get_DagBuilder_name_from_string_pointer(
            dag_builder_ctor_as_str
        )
        exp = "C3a"
        self.assert_equal(act, exp)

    def test3(self) -> None:
        dag_builder_ctor_as_str = (
            "dataflow_lemonade.pipelines.C5.C5b_pipeline.C5b_DagBuilder"
        )
        act = dtfcorutil.get_DagBuilder_name_from_string_pointer(
            dag_builder_ctor_as_str
        )
        exp = "C5b"
        self.assert_equal(act, exp)
