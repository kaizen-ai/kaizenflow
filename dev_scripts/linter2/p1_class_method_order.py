#!/usr/bin/env python
import argparse
import dataclasses
import enum
import logging
import re
from typing import Any, List, Tuple, Union

import dev_scripts.linter2.base as lntr
import dev_scripts.linter2.utils as utils
import helpers.dbg as dbg
import helpers.io_ as io_
import helpers.parser as prsr

_LOG = logging.getLogger(__name__)


@dataclasses.dataclass
class _IncorrectPositionNode:
    current_line_num: int
    correct_line_num: int
    node: "_Node"


def _is_class_declaration(line: str) -> str:
    """Check if a line declares a class.

    If so, it returns the class name. Else an empty string
    """
    regex = r"^\s*class (.*?)[\(:]"
    m = re.match(regex, line)
    if m is not None:
        class_name = m.group(1)
        return class_name
    return ""


def _is_function_declaration(line: str) -> str:
    """Check if a line defines a function.

    If so, it returns the function name. Else it returns an empty
    string.
    """
    regex = r"^\s*def (.*?)\(.*\).*:"
    m = re.match(regex, line)
    if m is not None:
        function_name = m.group(1)
        return function_name
    return ""


def _is_decorator(line: str) -> bool:
    """Check if a decorator is present on a line."""
    regex = r"\s*@.*"
    m = re.match(regex, line)
    return m is not None


class _Node:
    """A node represents either a class, function or raw text."""

    class Type(enum.Enum):
        CLS = 1
        FUNC = 2
        TEXT = 3
        ALL = 4

    def __init__(self, name: str, line: str, line_num: int, t: "_Node.Type"):
        """Represent either a class, function or raw text.

        :param name: the function or class name
        :param line: the complete line on which the node was found
        :param t: class, function or raw text
        """
        self.type = t
        self._children: List["_Node"] = list()
        # The complete line on which this node was found.
        self.line = line
        self.line_num = line_num
        self.name = name
        # All lines between this node and the next node, the body of functions/classes.
        self.body: List[str] = list()
        self.decorators: List[str] = list()
        self._indent: Union[None, int] = None

    def __repr__(self) -> str:
        types_as_str = {
            _Node.Type.FUNC: "function",
            _Node.Type.CLS: "class",
            _Node.Type.TEXT: "misc",
        }
        return f"{types_as_str[self.type]} <{self.name}>"

    def __eq__(self, other: Union[Any, "_Node"]) -> bool:
        if not isinstance(other, _Node):
            return False
        return (
            self.line_num == other.line_num
            and self.line == other.line
            and self.name == other.name
            and self.body == other.body
            and self.decorators == other.decorators
        )

    @property
    def indentation(self) -> int:
        if self._indent is None:
            self._indent = len(self.line) - len(self.line.lstrip(" "))
        return self._indent

    @property
    def ordered_children(self) -> List["_Node"]:
        if self.type != _Node.Type.CLS:
            pass
            # \_LOG.warning("You tried to order a node that isn't a class. This can possibly
            # mess up the formatting.")
        return sorted(self.get_children(), key=self._sorting_key)

    def get_children(self, t: "_Node.Type" = Type.ALL) -> List["_Node"]:
        if t == _Node.Type.ALL:
            return self._children
        return [child for child in self._children if child.type == t]

    def set_children(self, new_children: List["_Node"]) -> None:
        self._children = new_children

    def add_child(self, child: "_Node") -> None:
        self._children.append(child)

    def add_to_body(self, line: str) -> None:
        self.body.append(line)

    def add_decorator(self, decorator: str) -> None:
        self.decorators.append(decorator)

    def to_lines(self) -> List[str]:
        """Convert the _Node structure back to lines."""
        lines: List[str] = self.decorators + [self.line] + self.body
        for child in self.get_children():
            lines.extend(child.to_lines())
        return lines

    def check_method_order(self) -> List["_Node"]:
        """Check the order of the methods of a class."""
        current_order = self.get_children()
        correct_order = self.ordered_children
        offending = []

        while current_order and correct_order:
            current_node = current_order.pop(0)
            correct_node = correct_order.pop(0)
            # we only have to check magic and private methods. If those are in the correct
            # position, public methods will also have to be correctly positioned.
            if current_node != correct_node and current_node.name.startswith("_"):
                offending.append(current_node)
        return offending

    def _find_child_by_name(self, name: str) -> Union[None, "_Node"]:
        return next(
            (node for node in self.get_children() if node.name == name), None
        )

    @staticmethod
    def _sorting_key(item: "_Node") -> int:
        """Sort in order of: classes > __init__ > magic methods > public
        methods > private methods."""
        if item.name == "__init__":
            return 1
        if item.type == _Node.Type.CLS:
            return 0
        if item.name.startswith("__") and item.name.endswith("__"):
            return 2
        if item.name.startswith("_"):
            return 4
        return 3


def _node_from_line(line: str, line_num: int) -> Union[_Node, str]:
    """Construct a Node object from a line, if it declares a function or class.

    :param line: the line from which the object needs to be constructed
    :return: returns the line if no function/class was declared, else it returns
             an instantiated Node object
    """
    class_name: str = _is_class_declaration(line)
    function_name: str = _is_function_declaration(line)
    new_node: Union[_Node, str]
    if class_name:
        new_node = _Node(class_name, line, line_num, _Node.Type.CLS)
    elif function_name:
        new_node = _Node(function_name, line, line_num, _Node.Type.FUNC)
    else:
        new_node = line
    return new_node


def _find_parent_node(nodes: List[_Node], indent: int) -> Union[None, _Node]:
    """Find the parent node, based on level of indentation.

    We assume that classes can be the parents of functions and other classes and functions can
    be the parents of other functions and classes. Searches over the nodes and their children
    recursively.

    :param nodes: nodes to check on
    :param indent: level of indentation of the node for which the parent needs to be found
    :return: the parent node if found, else None
    """
    # Only classes and functions can be parents.
    filtered_nodes: List[_Node] = [
        n for n in nodes if n.type in [_Node.Type.CLS, _Node.Type.FUNC]
    ]
    if not filtered_nodes:
        return None
    # The parent has to be the last from the list.
    pn: _Node = filtered_nodes[-1]
    if pn.indentation == indent - 4:
        return pn
    return _find_parent_node(pn.get_children(), indent)


def _extract_decorator(
    line: str, next_line_is_decorator: bool
) -> Tuple[Union[None, str], bool]:
    is_decorator = _is_decorator(line)
    # support for multi-line decorators
    if (  # pylint: disable=no-else-return
        is_decorator and "(" in line
    ) or next_line_is_decorator:
        chars_to_reverse = -3 if line.endswith("\n") else -1
        next_line_is_decorator = line[chars_to_reverse] != ")"
        return line, next_line_is_decorator
    elif is_decorator:
        # Support for simple decorators, i.e. @staticmethod or @abstractmethod.
        return line, False
    return None, False


def _lines_to_nodes(lines: List[str]) -> List[_Node]:
    """Create a list of nodes from a list of lines.

    All nodes in the list, are the root (parentless?) nodes. Where appropriate,
     the root nodes have children.

    :param lines: the lines from which to construct the nodes
    :return: a list of nodes representing the lines
    """
    root_nodes: List[_Node] = list()
    decorators: List[str] = list()
    last_node: Union[None, _Node] = None
    parent_node: Union[None, _Node] = None
    previous_indent: Union[None, int] = None
    next_line_is_decorator: bool = False
    for line_num, line in enumerate(lines, 1):
        decorator_line, next_line_is_decorator = _extract_decorator(
            line, next_line_is_decorator
        )
        if decorator_line is not None:
            decorators.append(decorator_line)
            continue
        # Unless we're in the body of a function/class, it is safe to assume that the
        # parent node has changed if the level of indentation has moved.
        current_indent: int = len(line) - len(line.lstrip(" "))
        if current_indent != previous_indent:
            parent_node = _find_parent_node(root_nodes, current_indent)
        #
        node: Union[_Node, str] = _node_from_line(line, line_num)
        if isinstance(node, str):
            if last_node is not None:
                # all lines from within a class(basically only class variables) and all lines from
                # within functions
                last_node.add_to_body(line)
            else:
                # create a misc node to make sure all lines before function/class nodes(shebangs,
                # imports, etc) are also saved
                last_node = node = _Node("", line, line_num, _Node.Type.TEXT)
                root_nodes.append(node)
        elif isinstance(node, _Node):
            if parent_node is None:
                # If there is no current parent node, the new node has to be the parent.
                parent_node = node
            while decorators:
                node.add_decorator(decorators.pop(0))
            if current_indent == 0:
                root_nodes.append(node)
            else:
                parent_node.add_child(node)
            #
            last_node = node
        previous_indent = current_indent
    return root_nodes


def _nodes_to_lines_in_correct_order(nodes: List[_Node]) -> List[str]:
    updated_lines: List[str] = list()
    for node in nodes:
        if node.type == _Node.Type.CLS:
            node.set_children(node.ordered_children)
        updated_lines.extend(node.to_lines())
    return updated_lines


def _class_method_order_enforcer(lines: List[str]) -> List[str]:
    """Expect to be called from `_modify_file_lines`"""
    root_nodes = _lines_to_nodes(lines)
    updated_lines = _nodes_to_lines_in_correct_order(root_nodes)
    return updated_lines


def _class_method_order_detector(file_name: str, lines: List[str]) -> List[str]:
    root_nodes = _lines_to_nodes(lines)
    offending: List[_Node] = list()

    for node in root_nodes:
        if node.type not in [_Node.Type.CLS, _Node.Type.FUNC]:
            continue
        offending.extend(node.check_method_order())

    return [
        f"{file_name}:{off.line_num}: method `{off.name}`"
        f" is located on the wrong line"
        for off in offending
    ]


class _P1ClassMethodOrder(lntr.Action):
    """Detect class methods that are in the wrong order."""

    def __init__(self, executable: str, enforce: bool):
        self._enforce = enforce
        super().__init__(executable)

    def check_if_possible(self) -> bool:
        return True

    def _execute(self, file_name: str, pedantic: int) -> List[str]:
        _ = pedantic
        if not utils.is_py_file(file_name):
            _LOG.debug("Skipping file_name='%s'", file_name)
            return []
        lines = io_.from_file(file_name).split("\n")
        output: List[str] = []
        if not self._enforce:
            output.extend(_class_method_order_detector(file_name, lines))
        else:
            updated_lines = _class_method_order_enforcer(lines)
            utils.write_file_back(file_name, lines, updated_lines)
        return output


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "-e",
        "--enforce",
        action="store_true",
        default=False,
        help="Enforce method order",
    )
    parser.add_argument(
        "files", nargs="+", action="store", type=str, help="Files to process",
    )
    prsr.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level)
    action = _P1ClassMethodOrder("", args.enforce)
    lntr.run_action(action, args.files)


if __name__ == "__main__":
    _main(_parse())
