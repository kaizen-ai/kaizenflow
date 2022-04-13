import logging
from typing import Any, Dict, List

import pandas as pd

import core.config as cconfig
import dataflow.core.dag_adapter as dtfcodaada
import dataflow.core.dag_builder as dtfcodabui
import dataflow.core.dag_builder_example as dtfcdabuex
import dataflow.core.node as dtfcornode
import dataflow.core.nodes.sinks as dtfconosin
import dataflow.core.nodes.sources as dtfconosou
import helpers.hpandas as hpandas
import helpers.hprint as hprint
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)


# #############################################################################


def _get_data() -> pd.DataFrame:
    """
    Generate random data.
    """
    num_cols = 2
    seed = 42
    date_range_kwargs = {
        "start": pd.Timestamp("2010-01-01"),
        "end": pd.Timestamp("2010-01-10"),
        "freq": "1B",
    }
    data = hpandas.get_random_df(
        num_cols, seed=seed, date_range_kwargs=date_range_kwargs
    )
    return data


class TestDagAdapter1(hunitest.TestCase):
    def helper(
        self,
        dag_builder: dtfcodabui.DagBuilder,
        overriding_config: Dict[str, Any],
        nodes_to_insert: List[dtfcornode.Node],
        nodes_to_append: List[dtfcornode.Node],
    ) -> None:
        txt = []
        # Build the `DagAdapter`.
        txt.append(hprint.frame("dag_builder"))
        txt.append(str(dag_builder))
        #
        dag_adapter = dtfcodaada.DagAdapter(
            dag_builder, overriding_config, nodes_to_insert, nodes_to_append
        )
        txt.append(hprint.frame("dag_adapter"))
        txt.append(str(dag_adapter))
        # Compute the final DAG.
        config = dag_adapter.get_config_template()
        _LOG.debug("config=\n%s", config)
        dag = dag_adapter.get_dag(config)
        txt.append(hprint.frame("final dag"))
        txt.append(str(dag))
        # Check.
        txt = "\n".join(txt)
        self.check_string(txt, purify_text=True)

    def test1(self) -> None:
        """
        Adapt a DAG injecting a data source and appending a `WriteDf` node.
        """
        overriding_config = cconfig.Config()
        # Configure a `DataSourceNode`.
        overriding_config["load_prices"] = {
            "func": lambda x: x,
        }
        overriding_config["write_df"] = {
            "dir_name": "here",
        }
        # Do not insert any node.
        nodes_to_insert: List = []
        # Append a `WriteDf` node.
        nodes_to_append = []
        stage = "write_df"
        node_ctor = dtfconosin.WriteDf
        nodes_to_append.append((stage, node_ctor))
        #
        dag_builder = dtfcdabuex.LoadPrices_DagBuilder()
        # Check.
        self.helper(
            dag_builder, overriding_config, nodes_to_insert, nodes_to_append
        )

    def test2(self) -> None:
        """
        Adapt a DAG inserting a node.
        """
        overriding_config = cconfig.Config()
        # Configure a `DataSourceNode`.
        overriding_config["load_prices"] = {
            "func": _get_data,
        }
        # Insert one node.
        nodes_to_insert = []
        stage = "load_prices"
        node_ctor = dtfconosou.FunctionDataSource
        nodes_to_insert.append((stage, node_ctor))
        # Do not append any node.
        nodes_to_append: List = []
        #
        dag_builder = dtfcdabuex.ReturnsBuilder()
        # Check.
        self.helper(
            dag_builder, overriding_config, nodes_to_insert, nodes_to_append
        )
