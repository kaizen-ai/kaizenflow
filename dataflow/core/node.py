"""
Import as:

import dataflow.core.node as dtfcornode
"""
import abc
import logging
from typing import Any, Dict, List, Optional

import helpers.dbg as hdbg

_LOG = logging.getLogger(__name__)


# #############################################################################
# Core node classes
# #############################################################################

# We use a string to represent a node's unique identifier. This type helps
# improve the interface and make the code more readable (e.g., `Dict[NodeId, ...]`
# instead of `Dict[str, ...]`).
NodeId = str

# Name of a Node's method, e.g., `fit` or `predict`.
Method = str

# Mapping between the name of an output of a node and the corresponding stored value.
NodeOutput = Dict[str, Any]


# TODO(gp): This seems private -> _AbstractNode.
class NodeInterface(abc.ABC):
    """
    Abstract node class for creating DAGs of functions.

    Common use case: `Node`s wrap functions with a common method, e.g., `fit()`.

    This class provides some convenient introspection (input/output names)
    accessors and, importantly, a unique identifier (`nid`) for building
    graphs of nodes. The `nid` is also useful for config purposes.

    For nodes requiring fit/transform, we can subclass/provide a mixin with
    the desired methods.
    """

    # TODO(gp): Are inputs / output without names useful? If not we can simplify the
    #   interface.
    def __init__(
        self,
        nid: NodeId,
        inputs: Optional[List[str]] = None,
        outputs: Optional[List[str]] = None,
    ) -> None:
        """
        Constructor.

        :param nid: node identifier. Should be unique in a graph.
        :param inputs: list-like string names of `input_names`. `None` for no names.
        :param outputs: list-like string names of `output_names`. `None` for no names.
        """
        hdbg.dassert_isinstance(nid, NodeId)
        hdbg.dassert(nid, "Empty string chosen for unique nid!")
        self._nid = nid
        self._input_names = self._init_validation_helper(inputs)
        self._output_names = self._init_validation_helper(outputs)

    @property
    def nid(self) -> NodeId:
        return self._nid

    # TODO(gp): We might want to do getter only.
    @property
    def input_names(self) -> List[str]:
        return self._input_names

    @property
    def output_names(self) -> List[str]:
        return self._output_names

    # TODO(gp): Consider using the more common approach with `_check_validity()`.
    @staticmethod
    def _init_validation_helper(items: Optional[List[str]]) -> List[str]:
        """
        Ensure that items are valid and returns the validated items.
        """
        if items is None:
            return []
        # Make sure the items are all non-empty strings.
        for item in items:
            hdbg.dassert_isinstance(item, str)
            hdbg.dassert_ne(item, "")
        hdbg.dassert_no_duplicates(items)
        return items


# TODO(gp): Should we merge this with _AbstractNode? There is lots of class
#  hierarchy (NodeInterface -> Node -> FitPredictNode) that doesn't seem really
#  used / useful any more.
class Node(NodeInterface):
    """
    A node class that stores and retrieves its output values on a "per-method"
    basis.

    E.g., for each method (e.g., "fit" and "predict") returns a value
    for each output.
    """

    def __init__(
        self,
        nid: NodeId,
        inputs: Optional[List[str]] = None,
        outputs: Optional[List[str]] = None,
    ) -> None:
        """
        Implement the same interface as `NodeInterface`.
        """
        super().__init__(nid, inputs, outputs)
        # Dictionary method name -> output node name -> output.
        self._output_vals: Dict[Method, NodeOutput] = {}

    # TODO(gp): name -> output_name
    def get_output(self, method: Method, name: str) -> Any:
        """
        Return the value of output `name` for the requested `method`.
        """
        hdbg.dassert_in(
            method,
            self._output_vals.keys(),
            "%s of node %s has no output!",
            method,
            self.nid,
        )
        hdbg.dassert_in(
            name,
            self.output_names,
            "%s is not an output of node %s!",
            name,
            self.nid,
        )
        return self._output_vals[method][name]

    def get_outputs(self, method: Method) -> NodeOutput:
        """
        Return all the output values for the requested `method`.

        E.g., for a method "fit" it returns, "df_out" -> pd.DataFrame
        """
        hdbg.dassert_in(method, self._output_vals.keys())
        return self._output_vals[method]

    # TODO(gp): name -> output_name
    def _store_output(self, method: Method, name: str, value: Any) -> None:
        """
        Store the output for `name` and the specific `method`.
        """
        hdbg.dassert_in(
            name,
            self.output_names,
            "%s is not an output of node %s!",
            name,
            self.nid,
        )
        # Create a dictionary of values for `method` if it doesn't exist.
        if method not in self._output_vals:
            self._output_vals[method] = {}
        # Assign the requested value.
        self._output_vals[method][name] = value
