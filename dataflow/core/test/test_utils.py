import logging


import pandas as pd

import dataflow.core.utils as dtfcorutil
import helpers.hunit_test as hunitest
from datetime import datetime

from numpy import *
import numpy as np

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
    """
    Test that the function correctly transforms a DataFrame into a multi-index DataFrame.
    """
    
    def test1(self) -> None:
        # Create a sample DataFrame.
        data = {
            'end_time': [
                '2022-01-04 09:01:00-05:00',
                '2022-01-04 09:01:00-05:00',
                '2022-01-04 09:02:00-05:00',
                '2022-01-04 09:02:00-05:00',
                '2022-01-04 09:03:00-05:00',
            ],
            'id': [13684, 17085, 13684, 17085, 13684],
            'close': [None, None, None, None, None],
            'volume': [0, 0, 0, 0, 0],
        }
        sample_df = pd.DataFrame(data)
        # Define the asset_id_col.
        asset_id_col = 'id'
        # Call the function to transform the DataFrame
        result_df = dtfcorutil.convert_to_multiindex(sample_df, asset_id_col)
        # Assert that the resulting DataFrame has the expected structure
        self.assertTrue(isinstance(result_df, pd.DataFrame))
