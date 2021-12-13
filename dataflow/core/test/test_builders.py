import logging

import numpy as np

import dataflow.core.builders_example as dtfcobuexa
import dataflow.core.runners as dtfcorrunn
import helpers.printing as hprint
import helpers.unit_test as hunitest

_LOG = logging.getLogger(__name__)


# #############################################################################


class TestArmaReturnsBuilder(hunitest.TestCase):
    """
    Test the ArmaReturnsBuilder pipeline.
    """

    def test1(self) -> None:
        dag_builder = dtfcobuexa.ArmaReturnsBuilder()
        config = dag_builder.get_config_template()
        dag_runner = dtfcorrunn.FitPredictDagRunner(config, dag_builder)
        result_bundle = dag_runner.fit()
        df_out = result_bundle.result_df
        str_output = (
            f"{hprint.frame('config')}\n{config}\n"
            f"{hprint.frame('df_out')}\n{hunitest.convert_df_to_string(df_out, index=True)}\n"
        )
        self.check_string(str_output)


class TestMvnReturnsBuilder(hunitest.TestCase):
    """
    Test the ArmaReturnsBuilder pipeline.
    """

    def test1(self) -> None:
        dag_builder = dtfcobuexa.MvnReturnsBuilder()
        config = dag_builder.get_config_template()
        dag_runner = dtfcorrunn.FitPredictDagRunner(config, dag_builder)
        result_bundle = dag_runner.fit()
        df_out = result_bundle.result_df
        expected_cols = [
            ("close.ret_0", "MN0"),
            ("close.ret_0", "MN1"),
            ("close.ret_0", "MN2"),
            ("close.ret_0", "MN3"),
            ("twap.ret_0", "MN0"),
            ("twap.ret_0", "MN1"),
            ("twap.ret_0", "MN2"),
            ("twap.ret_0", "MN3"),
            ("vwap.ret_0", "MN0"),
            ("vwap.ret_0", "MN1"),
            ("vwap.ret_0", "MN2"),
            ("vwap.ret_0", "MN3"),
            ("close", "MN0"),
            ("close", "MN1"),
            ("close", "MN2"),
            ("close", "MN3"),
            ("twap", "MN0"),
            ("twap", "MN1"),
            ("twap", "MN2"),
            ("twap", "MN3"),
            ("volume", "MN0"),
            ("volume", "MN1"),
            ("volume", "MN2"),
            ("volume", "MN3"),
            ("vwap", "MN0"),
            ("vwap", "MN1"),
            ("vwap", "MN2"),
            ("vwap", "MN3"),
        ]
        np.testing.assert_equal(df_out.columns.to_list(), expected_cols)
        np.testing.assert_equal(df_out.shape, (2960, 28))
        np.testing.assert_equal(df_out.dropna(how="all").shape, (702, 28))
