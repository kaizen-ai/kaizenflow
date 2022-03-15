import logging

import numpy as np

import core.config as cconfig
import dataflow.core.dag_adapter as dtfcodaada
import dataflow.core.dag_builder_example as dtfcdabuex
import dataflow.core.dag_runner as dtfcodarun
import dataflow.core.nodes.sources as dtfconosou
import helpers.hprint as hprint
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)


# #############################################################################


# TODO(gp): Factor out the common code in a _TestPipelineBuilder.
class TestArmaReturnsBuilder(hunitest.TestCase):
    """
    Test the `ArmaReturnsBuilder` pipeline.
    """

    def test1(self) -> None:
        dag_builder = dtfcdabuex.ArmaReturnsBuilder()
        config = dag_builder.get_config_template()
        #
        dag_runner = dtfcodarun.FitPredictDagRunner(config, dag_builder)
        result_bundle = dag_runner.fit()
        #
        df_out = result_bundle.result_df
        str_output = (
            f"{hprint.frame('config')}\n{config}\n"
            f"{hprint.frame('df_out')}\n{hunitest.convert_df_to_string(df_out, index=True)}\n"
        )
        self.check_string(str_output)

    def test_str1(self) -> None:
        dag_builder = dtfcdabuex.ArmaReturnsBuilder()
        act = str(dag_builder)
        self.check_string(act)


class TestMvnReturnsBuilder(hunitest.TestCase):
    """
    Test the `MvnReturnsBuilder` pipeline.
    """

    def test1(self) -> None:
        dag_builder = dtfcdabuex.MvnReturnsBuilder()
        #
        overriding_config = cconfig.Config()
        overriding_config["load_prices"] = {
            "frequency": "T",
            "start_date": "2010-01-04 09:30:00",
            "end_date": "2010-01-14 16:05:00",
            "dim": 4,
            "target_volatility": 0.25,
            "seed": 247,
        }
        node = dtfconosou.MultivariateNormalDataSource
        nodes_to_insert = [("load_prices", node)]
        dag_builder = dtfcodaada.DagAdapter(
            dag_builder,
            overriding_config,
            nodes_to_insert,
            [],
        )
        #
        config = dag_builder.get_config_template()
        #
        dag_runner = dtfcodarun.FitPredictDagRunner(config, dag_builder)
        result_bundle = dag_runner.fit()
        #
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

    def test_str1(self) -> None:
        dag_builder = dtfcdabuex.MvnReturnsBuilder()
        act = str(dag_builder)
        self.check_string(act)