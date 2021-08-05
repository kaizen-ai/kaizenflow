"""
Import as:

import core.dataflow.visitors as cdtfv
"""

import collections
import copy
import logging
from typing import Any, List, MutableMapping

import helpers.dbg as dbg
import core.dataflow.core as cdtfc
from core.dataflow.nodes.base import FitPredictNode

_LOG = logging.getLogger(__name__)

# Mapping from node and method to some data.
Info = MutableMapping[cdtfc.Nid, MutableMapping[cdtfc.Method, Any]]


def extract_info(dag: cdtfc.DAG, methods: List[cdtfc.Method]) -> Info:
    """
    Extract node info from each DAG node.

    :param dag: dataflow DAG. Node info is populated upon running.
    :param methods: `Node` method infos to extract
    :return: nested `OrderedDict`
    """
    dbg.dassert_isinstance(dag, cdtfc.DAG)
    dbg.dassert_isinstance(methods, list)
    info = collections.OrderedDict()
    # Scan the nodes.
    for nid in dag.dag.nodes():
        node_info = collections.OrderedDict()
        node = dag.get_node(nid)
        # Extract the info for each method.
        for method in methods:
            method_info = node.get_info(method)
            node_info[method] = copy.copy(method_info)
        # TODO(gp): Not sure about the double copy. Maybe a single deepcopy is enough.
        info[nid] = copy.copy(node_info)
    return info


# TODO(gp): We could save / load state also of DAGs with some stateless Node.
def get_fit_state(dag: cdtfc.DAG) -> Info:
    """
    Obtain node state learned during fit.

    :param dag: dataflow DAG consisting of `FitPredictNode`s
    :return: result of node `get_fit_state()` keyed by nid
    """
    dbg.dassert_isinstance(dag, cdtfc.DAG)
    fit_state = collections.OrderedDict()
    for nid in dag.dag.nodes():
        node = dag.get_node(nid)
        # Save the info for the fit state.
        dbg.dassert_isinstance(node, FitPredictNode)
        node_fit_state = node.get_fit_state()
        fit_state[nid] = copy.copy(node_fit_state)
    return fit_state


def set_fit_state(dag: cdtfc.DAG, fit_state: collections.OrderedDict) -> None:
    """
    Initialize a DAG with pre-fit node state.

    :param dag: dataflow DAG consisting of `FitPredictNode`s
    :param fit_state: result of node `get_fit_state()` keyed by nid
    """
    dbg.dassert_isinstance(dag, cdtfc.DAG)
    dbg.dassert_isinstance(fit_state, collections.OrderedDict)
    dbg.dassert_eq(len(dag.dag.nodes()), len(fit_state.keys()))
    # Scan the nodes.
    for nid in dag.dag.nodes():
        node = dag.get_node(nid)
        # Set the info for the fit state.
        dbg.dassert_isinstance(node, FitPredictNode)
        dbg.dassert_in(nid, fit_state.keys())
        node_fit_state = copy.copy(fit_state[nid])
        node.set_fit_state(node_fit_state)
