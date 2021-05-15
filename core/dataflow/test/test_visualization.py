import logging
import os


import core.dataflow as dtf
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class Test_dataflow_core_visualization1(hut.TestCase):

    def test_draw1(self) -> None:
        """
        Build a DAG and draw it in IPython.
        """
        dag = self._build_dag()
        _ = dtf.draw(dag)

    def test_draw_to_file1(self) -> None:
        """
        Build a DAG, draw it, and save the result in a file.
        """
        dag = self._build_dag()
        # Save to file.
        dir_name = self.get_scratch_space()
        file_name = os.path.join(dir_name, "plot.png")
        dtf.draw_to_file(dag, file_name)
        _LOG.debug("file_name=%s", file_name)
        # Check that the output file exists.
        self.assertTrue(os.path.exists(file_name))

    @staticmethod
    def _build_dag() -> dtf.DAG:
        dag = dtf.DAG()
        n1 = dtf.Node("n1")
        dag.add_node(n1)
        return dag
