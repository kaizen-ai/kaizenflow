"""
Import as:

import dataflow.core.dag as dtfcordag
"""
import itertools
import json
import logging
import os
from typing import Any, Dict, List, Optional, Tuple, Union

import networkx as networ
import pandas as pd
from tqdm.autonotebook import tqdm

import dataflow.core.node as dtfcornode
import helpers.hdbg as hdbg
import helpers.hio as hio
import helpers.hlist as hlist
import helpers.hlogging as hloggin
import helpers.hpandas as hpandas
import helpers.hprint as hprint
import helpers.htimer as htimer
import helpers.hwall_clock_time as hwacltim

_LOG = logging.getLogger(__name__)

# #############################################################################
# Class for creating and executing a DAG of nodes.
# #############################################################################


DagOutput = Dict[dtfcornode.NodeId, dtfcornode.NodeOutput]


class DAG:
    """
    Class for building DAGs using Nodes.

    The DAG manages node execution and storage of outputs (within
    executed nodes).
    """

    # TODO(gp): -> name: str to simplify the interface
    def __init__(
        self,
        name: Optional[str] = None,
        mode: Optional[str] = None,
        *,
        save_node_interface: str = "",
        profile_execution: bool = False,
        dst_dir: Optional[str] = None,
    ) -> None:
        """
        Create a DAG.

        :param name: optional str identifier
        :param mode: determines how to handle an attempt to add a node that already
            belongs to the DAG:
            - "strict": asserts
            - "loose": deletes old node (also removes edges) and adds new node. This
              is useful for interactive notebooks and debugging
        :param save_node_interface, profile_execution, dst_dir: see `set_debug_mode()`
        """
        self._dag = networ.DiGraph()
        #
        if name is not None:
            hdbg.dassert_isinstance(name, str)
        self._name = name
        #
        if mode is None:
            mode = "strict"
        hdbg.dassert_in(
            mode, ["strict", "loose"], "Unsupported mode %s requested!", mode
        )
        self._mode = mode
        #
        self.set_debug_mode(save_node_interface, profile_execution, dst_dir)

    def set_debug_mode(self,
        save_node_interface: str,
        profile_execution: bool,
        dst_dir: Optional[str],
    ) -> None:
        """
        Set the debug parameters see

        Sometimes it's difficult to pass these parameters (e.g., through a
        `DagBuilder`) so we allow to set them after construction.

        :param save_node_interface: store the values at the interface of the nodes
            into a directory `dst_dir`. Disclaimer: the amount of data generate can
            be huge
            - ``: save no information
            - `stats`: save high level information about the node interface
            - `df_as_csv`: save the full content of the node interface, using CSV for
              dataframes
            - `df_as_parquet`: like `df_as_csv` but using Parquet for dataframes
        :param profile_execution: if not `None`, store information about the
            execution of the nodes
        :param dst_dir: directory to save node interface and execution profiling info
        """
        hdbg.dassert_in(save_node_interface, ("", "stats", "df_as_csv", "df_as_parquet"))
        self._save_node_interface = save_node_interface
        # To process the profiling info in a human consumable form:
        # ```
        # ls -tr -1 tmp.dag_profile/*after* | xargs -n 1 -i sh -c 'echo; echo; echo "# {}"; cat {}'
        # ```
        self._profile_execution = profile_execution
        self._dst_dir = dst_dir
        if self._dst_dir:
            hio.create_dir(self._dst_dir, incremental=False)
        if self._save_node_interface or self._profile_execution:
            _LOG.warning("Setting up debug mode: " +
                hprint.to_str("save_node_interface profile_execution dst_dir"))
            hdbg.dassert_is_not(
                dst_dir, None, "Need to specify a directory to save the data"
            )

    def __str__(self) -> str:
        """
        Return a short representation for user.

        E.g.,
        ```
        name=None
        mode=strict
        nodes=[('n1', {'stage': <dataflow.core.node.Node object at 0x>})]
        edges=[]
        ```
        """
        txt = []
        txt.append(f"name={self._name}")
        txt.append(f"mode={self._mode}")
        txt.append("nodes=" + str(self.dag.nodes(data=True)))
        txt.append("edges=" + str(self.dag.edges(data=True)))
        return "\n".join(txt)

    def __repr__(self) -> str:
        """
        Return a detailed representation for debugging.

        E.g.,
        ```
        name=None
        mode=strict
        json=
          {
              "directed": true,
              ...
              "nodes": [
                  {
                      "id": "n1",
                      "stage": "Node"
                  }
              ]
          }
        ```
        """
        txt = []
        txt.append(f"name={self._name}")
        txt.append(f"mode={self._mode}")
        txt.append("json=")
        txt.append(hprint.indent(self._to_json(), 2))
        return "\n".join(txt)

    # TODO(gp): A bit confusing since other classes have `dag / get_dag` method that
    #  returns a DAG. Also the code does `dag.dag`. Maybe -> `nx_dag()` to say that
    #  we are extracting the networkx data structures.
    @property
    def dag(self) -> networ.DiGraph:
        return self._dag

    # TODO(*): Should we force to always have a name? So mypy can perform more
    #  checks.
    @property
    def name(self) -> Optional[str]:
        return self._name

    @property
    def mode(self) -> str:
        return self._mode

    def add_node(self, node: dtfcornode.Node) -> None:
        """
        Add `node` to the DAG.

        Rely upon the unique nid for identifying the node.
        """
        # In principle, `NodeInterface` could be supported; however, to do so,
        # the `run` methods below would need to be suitably modified.
        hdbg.dassert_issubclass(
            node, dtfcornode.Node, "Only DAGs of class `Node` are supported!"
        )
        # NetworkX requires that nodes be hashable and uses hashes for
        # identifying nodes. Because our Nodes are objects whose hashes can
        # change as operations are performed, we use the `Node.nid` as the
        # NetworkX node and the `Node` instance as a `node attribute`, which we
        # identifying internally with the keyword `stage`.
        #
        # Note that this usage requires that nid's be unique within a given
        # DAG.
        if self.mode == "strict":
            hdbg.dassert(
                not self._dag.has_node(node.nid),
                "A node with nid=%s already belongs to the DAG!",
                node.nid,
            )
        elif self.mode == "loose":
            # If a node with the same id already belongs to the DAG:
            #   - Remove the node and all of its successors (and their incident
            #     edges)
            #   - Add the new node to the graph.
            # This is useful for notebook research flows, e.g., rerunning
            # blocks that build the DAG incrementally.
            if self._dag.has_node(node.nid):
                _LOG.warning(
                    "Node `%s` is already in DAG. Removing existing node, "
                    "successors, and all incident edges of such nodes. ",
                    node.nid,
                )
                # Remove node successors.
                for nid in networ.descendants(self._dag, node.nid):
                    _LOG.warning("Removing nid=%s", nid)
                    self.remove_node(nid)
                # Remove node.
                _LOG.warning("Removing nid=%s", node.nid)
                self.remove_node(node.nid)
        else:
            hdbg.dfatal("Invalid mode='%s'", self.mode)
        # Add node.
        self._dag.add_node(node.nid, stage=node)

    def get_node(self, nid: dtfcornode.NodeId) -> dtfcornode.Node:
        """
        Implement a convenience node accessor.

        :param nid: unique node id
        """
        hdbg.dassert_isinstance(nid, dtfcornode.NodeId)
        hdbg.dassert(self._dag.has_node(nid), "Node `%s` is not in DAG!", nid)
        return self._dag.nodes[nid]["stage"]  # type: ignore

    def remove_node(self, nid: dtfcornode.NodeId) -> None:
        """
        Remove node from DAG and clear any connected edges.
        """
        hdbg.dassert(self._dag.has_node(nid), "Node `%s` is not in DAG!", nid)
        self._dag.remove_node(nid)

    def connect(
        self,
        parent: Union[
            Tuple[dtfcornode.NodeId, dtfcornode.NodeId], dtfcornode.NodeId
        ],
        child: Union[
            Tuple[dtfcornode.NodeId, dtfcornode.NodeId], dtfcornode.NodeId
        ],
    ) -> None:
        """
        Add a directed edge from parent node output to child node input.

        Raise if the requested edge is invalid or forms a cycle.

        If this is called multiple times on the same nid's but with different
        output/input pairs, the additional input/output pairs are simply added
        to the existing edge (the previous ones are not overwritten).

        :param parent: tuple of the form (nid, output) or nid if it has a single
            output
        :param child: tuple of the form (nid, input) or just nid if it has a single
            input
        """
        # Automatically infer output name when the parent has only one output.
        # Ensure that parent node belongs to DAG (through `get_node` call).
        if isinstance(parent, tuple):
            parent_nid, parent_out = parent
        else:
            parent_nid = parent
            parent_out = hlist.assert_single_element_and_return(
                self.get_node(parent_nid).output_names
            )
        hdbg.dassert_in(parent_out, self.get_node(parent_nid).output_names)
        # Automatically infer input name when the child has only one input.
        # Ensure that child node belongs to DAG (through `get_node` call).
        if isinstance(child, tuple):
            child_nid, child_in = child
        else:
            child_nid = child
            child_in = hlist.assert_single_element_and_return(
                self.get_node(child_nid).input_names
            )
        hdbg.dassert_in(child_in, self.get_node(child_nid).input_names)
        # Ensure that `child_in` is not already hooked up to an output.
        for nid in self._dag.predecessors(child_nid):
            hdbg.dassert_not_in(
                child_in,
                self._dag.get_edge_data(nid, child_nid),
                "`%s` already receiving input from node %s",
                child_in,
                nid,
            )
        # Add the edge along with an `edge attribute` indicating the parent
        # output to connect to the child input.
        kwargs = {child_in: parent_out}
        self._dag.add_edge(parent_nid, child_nid, **kwargs)
        # If adding the edge causes the DAG property to be violated, remove the
        # edge and raise an error.
        if not networ.is_directed_acyclic_graph(self._dag):
            self._dag.remove_edge(parent_nid, child_nid)
            hdbg.dfatal(
                f"Creating edge {parent_nid} -> {child_nid} introduces a cycle!"
            )

    def get_sources(self) -> List[dtfcornode.NodeId]:
        """
        :return: list of nid's of source nodes
        """
        sources = []
        for nid in networ.topological_sort(self._dag):
            if not any(True for _ in self._dag.predecessors(nid)):
                sources.append(nid)
        return sources

    def get_unique_source(self) -> dtfcornode.NodeId:
        """
        Return the only source node, asserting if there is more than one.
        """
        sources = self.get_sources()
        hdbg.dassert_eq(
            len(sources),
            1,
            "There is more than one sink node %s in DAG",
            str(sources),
        )
        return sources[0]

    def get_sinks(self) -> List[dtfcornode.NodeId]:
        """
        :return: list of nid's of sink nodes
        """
        sinks = []
        for nid in networ.topological_sort(self._dag):
            if not any(True for _ in self._dag.successors(nid)):
                sinks.append(nid)
        return sinks

    def get_unique_sink(self) -> dtfcornode.NodeId:
        """
        Return the only sink node, asserting if there is more than one.
        """
        sinks = self.get_sinks()
        hdbg.dassert_eq(
            len(sinks),
            1,
            "There is more than one sink node %s in DAG",
            str(sinks),
        )
        return sinks[0]

    def run_dag(self, method: dtfcornode.Method) -> DagOutput:
        """
        Execute entire DAG.

        :param method: method of class `Node` (or subclass) to be executed for
            the entire DAG
        :return: dict keyed by sink node nid with values from node's
            `get_outputs(method)`
        """
        sinks = self.get_sinks()
        for nid in networ.topological_sort(self._dag):
            self._run_node(nid, method)
        return {sink: self.get_node(sink).get_outputs(method) for sink in sinks}

    def run_leq_node(
        self,
        nid: dtfcornode.NodeId,
        method: dtfcornode.Method,
        progress_bar: bool = True,
    ) -> dtfcornode.NodeOutput:
        """
        Execute DAG up to (and including) Node `nid` and return output.

        "leq" in the method name refers to the partial ordering on the vertices.
        This method runs a node if and only if there is a directed path from the
        node to `nid`. Nodes are run according to a topological sort.

        :param nid: desired terminal node for execution
        :param method: `Node` subclass method to be executed
        :return: the mapping from output name to corresponding value (i.e., the
            result of node `nid`'s `get_outputs(method)`
        """
        ancestors = filter(
            lambda x: x in networ.ancestors(self._dag, nid),
            networ.topological_sort(self._dag),
        )
        # The `ancestors` filter only returns nodes strictly less than `nid`,
        # and so we need to add `nid` back.
        nids = itertools.chain(ancestors, [nid])
        # Execute all the ancestors of `nid`.
        if progress_bar:
            nids = tqdm(list(nids), desc="run_leq_node")
        for id_, pred_nid in enumerate(nids):
            _LOG.debug("Executing node '%s'", pred_nid)
            self._run_node(id_, pred_nid, method)
        # Retrieve the output the node.
        node = self.get_node(nid)
        node_output = node.get_outputs(method)
        return node_output

    def _to_json(self) -> str:
        # Get internal networkx representation of the DAG.
        graph: networ.classes.digraph.DiGraph = self.dag
        nld = networ.readwrite.json_graph.node_link_data(graph)
        # Remove stages names from `node_link_data` dictionary since they refer to
        # `Node` objects, which are not JSON serializable.
        # E.g., `nld` looks like:
        #   {'directed': True,
        #    'graph': {},
        #    'links': [],
        #    'multigraph': False,
        #    'nodes': [{'id': 'n1',
        #               'stage': <dataflow.core.Node object at 0x...>}]}
        nld = nld.copy()
        for data in nld["nodes"]:
            data["stage"] = data["stage"].__class__.__name__
        # Print as JSON.
        json_nld = json.dumps(nld, indent=4, sort_keys=True)
        return json_nld

    def _write_system_stats_to_dst_dir(
        self,
        topological_id: int,
        nid: dtfcornode.NodeId,
        method: dtfcornode.Method,
        file_tag: str,
        *,
        extra_txt: str = ""
    ) -> None:
        """
        Write information about the system (e.g., time and memory) before running a
        node.

        The file has a format like
        `{dst_dir}/{method}.{topological_id}.{nid}.{file_tag}.txt`

        :param topological_id, nid, method: information about the node and its method
            to run
        :param file_tag: the tag to add to the file (e.g., "before_execution",
            "after_execution")
        """
        txt = []
        curr_timestamp = str(hwacltim.get_machine_wall_clock_time())
        txt.append(f"timestamp={curr_timestamp}")
        memory_as_str = str(hloggin.get_memory_usage_as_str(process=None))
        txt.append("memory=%s" % memory_as_str)
        if extra_txt:
            txt.append(extra_txt)
        txt = "\n".join(txt)
        # Report the information on the screen.
        _LOG.info("\n%s\n%s",
            hprint.frame("%s: method '%s' for node topological_id=%s nid='%s'"
                         % (file_tag, method, topological_id, nid)),
            txt,
        )
        # Save information to file.
        basename = f"{method}.{topological_id}.{nid}.{file_tag}.txt"
        file_name = os.path.join(self._dst_dir, basename)
        hio.to_file(file_name, txt)

    def _write_node_interface_to_dst_dir(
        self,
        topological_id: int,
        nid: dtfcornode.NodeId,
        method: dtfcornode.Method,
        output_name: str,
        obj: Any,
    ) -> None:
        """
        Write information about the system (e.g., time and memory) before running a
        node.

        The file has a format like:
        `{dst_dir}/{method}.{topological_id}.{nid}.{file_tag}.txt`
        """
        basename = f"{method}.{topological_id}.{nid}.{output_name}"
        file_name = os.path.join(self._dst_dir, basename)
        #
        if isinstance(obj, pd.Series):
            obj = pd.DataFrame(obj)
        if isinstance(obj, pd.DataFrame):
            df = obj
            # Save high level description about the df.
            txt = hpandas.df_to_str(df, print_dtypes=True, print_shape_info=True,
                                    print_memory_usage=True, print_nan_info=True)
            hio.to_file(file_name + ".txt", txt)
            # Save content of the df.
            if self._save_node_interface == "df_as_csv":
                df.to_csv(file_name + ".csv")
            elif self._save_node_interface == "df_as_parquet":
                import helpers.hparquet as hparque

                hparque.to_parquet(df, file_name + ".parquet")
        else:
            _LOG.warning(
                "Can't save node input / output of type '%s': %s",
                str(type(obj)),
                obj,
            )

    def _run_node(
        self,
        topological_id: int,
        nid: dtfcornode.NodeId,
        method: dtfcornode.Method,
    ) -> None:
        """
        Run the requested `method` on a single node.

        This method DOES NOT run (or re-run) ancestors of `nid`.
        """
        _LOG.debug(
            "\n%s",
            hprint.frame(
                "Executing method '%s' for node topological_id=%s nid='%s' ..."
                % (method, topological_id, nid)
            ),
        )
        # Save system info before execution of the node.
        if self._profile_execution:
            file_tag = "before_execution"
            self._write_system_stats_to_dst_dir(
                topological_id, nid, method, file_tag
            )
            run_node_dtimer = htimer.dtimer_start(logging.DEBUG, "run_node")
            run_node_dmemory = htimer.dmemory_start(logging.DEBUG, "run_node")
        # Retrieve the arguments needed to execute the `method` on the node.
        kwargs = {}
        for pred_nid in self._dag.predecessors(nid):
            kvs = self._dag.edges[[pred_nid, nid]]
            _LOG.debug("pred_nid=%s, nid=%s", pred_nid, nid)
            pred_node = self.get_node(pred_nid)
            for input_name, value in kvs.items():
                # Retrieve output from store.
                kwargs[input_name] = pred_node.get_output(method, value)
            # TODO(gp): Save info for inputs, if needed.
        _LOG.debug("kwargs are %s", kwargs)
        # Execute `node.method()`.
        with htimer.TimedScope(logging.DEBUG, "node_execution") as ts:
            node = self.get_node(nid)
            try:
                output = getattr(node, method)(**kwargs)
            except AttributeError as e:
                raise AttributeError(
                    f"An exception occurred in node '{nid}'\n{str(e)}"
                ) from e
        # Update the node.
        for output_name in node.output_names:
            value = output[output_name]
            node._store_output(  # pylint: disable=protected-access
                method, output_name, value
            )
            if self._save_node_interface:
                # Save info for the output of the node.
                self._write_node_interface_to_dst_dir(
                    topological_id, nid, method, output_name, value
                )
        # Save system info after execution the node.
        if self._profile_execution:
            file_tag = "after_execution"
            txt = []
            txt.append(ts.get_result())
            txt.append(htimer.dtimer_stop(run_node_dtimer)[0])
            txt.append(htimer.dmemory_stop(run_node_dmemory))
            txt = "\n".join(txt)
            self._write_system_stats_to_dst_dir(
                topological_id, nid, method, file_tag, extra_txt=txt,
            )
