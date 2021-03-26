import collections
import copy
import logging
from typing import List

import helpers.dbg as dbg

from core.dataflow.core import DAG

_LOG = logging.getLogger(__name__)


def extract_info(dag: DAG, methods: List[str]) -> collections.OrderedDict:
    """
    Extract node info from each DAG node.

    :param dag: dataflow DAG. Node info is populated upon running.
    :param methods: Node method infos to extract
    :return: nested OrderedDict
    """
    dbg.dassert_isinstance(dag, DAG)
    dbg.dassert_isinstance(methods, list)
    info = collections.OrderedDict()
    for nid in dag.dag.nodes():
        node_info = collections.OrderedDict()
        for method in methods:
            method_info = dag.get_node(nid).get_info(method)
            node_info[method] = copy.copy(method_info)
        info[nid] = copy.copy(node_info)
    return info


def get_fit_state(dag: DAG) -> collections.OrderedDict:
    """
    Obtain node state learned during fit.

    :param dag: dataflow DAG
    :return: result of node `get_fit_state()` keyed by nid
    """
    dbg.dassert_isinstance(dag, DAG)
    fit_state = collections.OrderedDict()
    for nid in dag.dag.nodes():
        node_fit_state = dag.get_node(nid).get_fit_state()
        fit_state[nid] = copy.copy(node_fit_state)
    return fit_state


def set_fit_state(dag: DAG, fit_state: collections.OrderedDict) -> None:
    """
    Initialize a DAG with pre-fit node state.

    :param dag: dataflow DAG
    :param fit_state: result of node `get_fit_state()` keyed by nid
    """
    dbg.dassert_isinstance(dag, DAG)
    dbg.dassert_isinstance(fit_state, collections.OrderedDict)
    dbg.dassert_eq(len(dag.dag.nodes()), len(fit_state.keys()))
    for nid in dag.dag.nodes():
        dbg.dassert_in(nid, fit_state.keys())
        node_fit_state = copy.copy(fit_state[nid])
        dag.get_node(nid).set_fit_state(node_fit_state)
