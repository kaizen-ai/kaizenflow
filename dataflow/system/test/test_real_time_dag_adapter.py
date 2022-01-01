import logging
import os

import pandas as pd

import dataflow.core as dtfcobuexa
import dataflow.system.real_time_dag_adapter as dtfsrtdaad
import helpers.printing as hprint
import helpers.unit_test as hunitest
import oms.portfolio_example as oporexam

_LOG = logging.getLogger(__name__)


class TestRealtimeDagAdapter1(hunitest.TestCase):
    """
    Test RealTimeDagAdapter building various DAGs.
    """

    def testMvnReturnsBuilder1(self) -> None:
        """
        Build a realtime DAG from `MvnReturnsBuilder()`.
        """
        txt = []
        # Build a DagBuilder.
        # TODO(Paul): Replace this with `MvnReturnsBuilder()`.
        dag_builder = dtfcobuexa.ReturnsBuilder()
        txt.append(hprint.frame("dag_builder"))
        txt.append(hprint.indent(str(dag_builder)))
        # Build a Portfolio.
        # TODO(Paul): Use a nontrivial event loop.
        event_loop = None
        initial_timestamp = pd.Timestamp("2000-01-01 09:35:00-05:00")
        portfolio = oporexam.get_simulated_portfolio_example1(
            event_loop, initial_timestamp
        )
        # Build a DagAdapter.
        dag_adapter = dtfsrtdaad.RealTimeDagAdapter(
            dag_builder, portfolio, "close"
        )
        txt.append(hprint.frame("dag_adapter"))
        txt.append(hprint.indent(str(dag_adapter)))
        # Compute the final DAG.
        config = dag_adapter.get_config_template()
        _LOG.debug("config=\n%s", config)
        dag = dag_adapter.get_dag(config)
        #
        file_name = os.path.join(self.get_scratch_space(), "dag.png")
        dtfcobuexa.draw_to_file(dag, file_name)
        txt.append(hprint.frame("final dag"))
        txt.append(str(dag))
        # Check.
        txt = "\n".join(txt)
        self.check_string(txt, purify_text=True)
