import logging

import core.dataflow as dtf
import helpers.printing as prnt
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class TestArmaReturnsBuilder(hut.TestCase):
    """
    Test the ArmaReturnsBuilder pipeline.
    """

    def test1(self) -> None:
        dag_builder = dtf.ArmaReturnsBuilder()
        config = dag_builder.get_config_template()
        dag_runner = dtf.FitPredictDagRunner(config, dag_builder)
        result_bundle = dag_runner.fit()
        df_out = result_bundle.result_df
        str_output = (
            f"{prnt.frame('config')}\n{config}\n"
            f"{prnt.frame('df_out')}\n{hut.convert_df_to_string(df_out, index=True)}\n"
        )
        self.check_string(str_output)
