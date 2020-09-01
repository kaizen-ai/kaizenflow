#!/usr/bin/env python
r"""Perform some p1 specific lints on python files

> p1_specific_lints.py sample_file1.py sample_file2.py
"""
import argparse
import ast
import dataclasses
import enum
import logging
import os
import re
from typing import Any, Callable, List, Tuple, Union

import typing_extensions

import dev_scripts.linter2.base as lntr
import dev_scripts.linter2.utils as utils
import helpers.dbg as dbg
import helpers.io_ as io_
import helpers.parser as prsr

_LOG = logging.getLogger(__name__)

# #############################################################################
# File Path Checks.
# #############################################################################


def _check_notebook_dir(file_name: str) -> str:
    """Check if that notebooks are under `notebooks` dir."""
    msg = ""
    if utils.is_ipynb_file(file_name):
        subdir_names = file_name.split("/")
        if "notebooks" not in subdir_names:
            msg = (
                "%s:1: each notebook should be under a 'notebooks' "
                "directory to not confuse pytest" % file_name
            )
    return msg


def _check_test_file_dir(file_name: str) -> str:
    """Check if test files are under `test` dir."""
    msg = ""
    # TODO(gp): A little annoying that we use "notebooks" and "test".
    if utils.is_py_file(file_name) and os.path.basename(file_name).startswith(
        "test_"
    ):
        if not utils.is_under_test_dir(file_name):
            msg = (
                "%s:1: test files should be under 'test' directory to "
                "be discovered by pytest" % file_name
            )
    return msg


def _check_notebook_filename(file_name: str) -> str:
    r"""Check notebook filenames start with `Master_` or match: `\S+Task\d+_...`"""
    msg = ""

    basename = os.path.basename(file_name)
    if utils.is_ipynb_file(file_name) and not any(
        [basename.startswith("Master_"), re.match(r"^\S+Task\d+_", basename)]
    ):
        msg = (
            f"{file_name}:1: "
            r"All notebook filenames start with `Master_` or match: `\S+Task\d+_...`"
        )
    return msg


def _check_file_path(file_name: str) -> List[str]:
    """Perform various checks based on the path of a file:

    - check that notebook files are under a `notebooks` dir
    - check that test files are under `test` dir
    """
    # Functions that take the filepath, and return an error message or an empty
    # string.
    FilePathCheck = Callable[[str], str]
    FILE_PATH_CHECKS: List[FilePathCheck] = [
        _check_notebook_dir,
        _check_test_file_dir,
        _check_notebook_filename,
    ]

    output: List[str] = []
    for func in FILE_PATH_CHECKS:
        msg = func(file_name)
        if msg:
            _LOG.warning(msg)
            output.append(msg)

    return output


# #############################################################################
# File Content Checks.
# #############################################################################


def _check_shebang(file_name: str, lines: List[str]) -> str:
    """Return warning if:

    - a python executable has no shebang
    - a python file has a shebang and isn't executable

    Note: the function ignores __init__.py files  & test code.
    """
    msg = ""
    if os.path.basename(file_name) == "__init__.py" or utils.is_test_code(
        file_name
    ):
        return msg

    shebang = "#!/usr/bin/env python"
    has_shebang = lines[0] == shebang
    is_executable = os.access(file_name, os.X_OK)

    if is_executable and not has_shebang:
        msg = f"{file_name}:1: any executable needs to start with a shebang '{shebang}'"
    elif not is_executable and has_shebang:
        msg = f"{file_name}:1: a non-executable can't start with a shebang."

    return msg


def _check_file_lines(file_name: str, lines: List[str]) -> List[str]:
    """Check file content, prints warnings if any issues are found.

    :param file_name: name of the file being checked
    :param lines: lines of content
    """

    class ContentCheck(typing_extensions.Protocol):
        """A function that takes a file's content, and return an error message
        or an empty string."""

        def __call__(self, file_name: str, lines: List[str]) -> Union[List, str]:
            # This definition is needed as typing.Callable doesn't support keyword arguments.
            # Ref: https://github.com/python/mypy/issues/1655.
            ...

    CONTENT_CHECKS: List[ContentCheck] = [
        _check_shebang,
    ]

    output: List[str] = []
    for check in CONTENT_CHECKS:
        msg: Union[str, list] = check(file_name=file_name, lines=lines)
        if msg:
            if isinstance(msg, str):
                output.append(msg)
            else:
                output.extend(msg)

    return output


# #############################################################################
# File Content Modifiers.
# #############################################################################


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
    # Support for multi-line decorators.
    if (is_decorator and "(" in line) or next_line_is_decorator:
        chars_to_reverse = -3 if line.endswith("\n") else -1
        next_line_is_decorator = line[chars_to_reverse] != ")"
        return line, next_line_is_decorator
    if is_decorator:
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

def _modify_file_lines(lines: List[str]) -> List[str]:
    """Modify multiple lines based on some rules, returns an updated list of
    line.

    :param lines: lines of content
    :return: updated lines of content
    """
    # Functions that take a line, and return a modified line.
    ContentModifier = Callable[[List[str]], List[str]]
    CONTENT_MODIFIERS: List[ContentModifier] = [
        # TODO(amr): enable this once it's tested enough. \_reflow_comments_in_lines.
    ]

    for modifier in CONTENT_MODIFIERS:
        lines = modifier(lines)
    return lines


# #############################################################################
# File Line Modifiers.
# #############################################################################


def _is_valid_python_statement(comments: List[str]) -> bool:
    joined = "\n".join(comments)
    try:
        ast.parse(joined)
    except SyntaxError:
        return False
    return True


# TODO (*): Is this function used anywhere?
def _capitalize(comment: str) -> str:
    return f"{comment[0:2].upper()}{comment[2::]}"


def _format_separating_line(
    line: str, min_num_chars: int = 6, line_width: int = 78
) -> str:
    """Transform a line int oa separating line if more than 6 # are found.

    :param line: line to format
    :param min_num_chars: minimum number of # to match after '# ' to decide
    this is a seperator line
    :param line_width: desired width for the seperator line
    :return: modified line
    """
    regex = r"(\s*\#)\s*([\#\=\-\<\>]){%d,}\s*$" % min_num_chars

    m = re.match(regex, line)
    if m:
        char = m.group(2)
        line = m.group(1) + " " + char * (line_width - len(m.group(1)))
    return line


def _modify_file_line_by_line(lines: List[str]) -> List[str]:
    """Modify each line based on some rules, returns an updated list of line.

    :param lines: lines of content
    :return: updated lines of content
    """
    # Functions that take a line, and return a modified line.
    LineModifier = Callable[[str], str]
    LINE_MODIFIERS: List[LineModifier] = [
        _format_separating_line,
        # TODO(amr): re-enable this for multi-line comments only. \_fix_comment_style,
        # TODO(gp): Remove empty lines in functions.
    ]

    txt_new: List[str] = []
    for line in lines:
        for modifier in LINE_MODIFIERS:
            line = modifier(line)
        txt_new.append(line)
    return txt_new


# #############################################################################
# File line checks.
# #############################################################################


def _warn_incorrectly_formatted_todo(
    file_name: str, line_num: int, line: str
) -> str:
    """Issues a warning for incorrectly formatted todo comments that don't
    match the format: (# TODO(assignee): (task).)"""
    msg = ""

    match = utils.parse_comment(line=line)
    if match is None:
        return msg

    comment = match.group(2)
    if not comment.lower().strip().startswith("todo"):
        return msg

    todo_regex = r"TODO\(\S+\): (.*)"

    match = re.search(todo_regex, comment)
    if match is None:
        msg = f"{file_name}:{line_num}: found incorrectly formatted TODO comment: '{comment}'"
    return msg


def _check_file_line_by_line(file_name: str, lines: List[str]) -> List[str]:
    """Check file line by line, prints warnings if any issues are found.

    :param file_name: name of the file being checked
    :param lines: lines of content
    """

    class LineCheck(typing_extensions.Protocol):
        """A function that takes a line, and return an error message or an
        empty string."""

        def __call__(self, file_name: str, line_num: int, line: str) -> str:
            # This definition is needed as typing.Callable doesn't support keyword arguments.
            # Ref: https://github.com/python/mypy/issues/1655.
            ...

    LINE_CHECKS: List[LineCheck] = [
        _warn_incorrectly_formatted_todo,
    ]

    output: List[str] = []

    for idx, line in enumerate(lines):
        for check in LINE_CHECKS:
            msg = check(file_name=file_name, line_num=idx + 1, line=line)
            if msg:
                output.append(msg)
    return output


# #############################################################################
# Action.
# #############################################################################


class _P1SpecificLints(lntr.Action):
    """Apply p1 specific lints."""

    def check_if_possible(self) -> bool:
        return True

    def _execute(self, file_name: str, pedantic: int) -> List[str]:
        _ = pedantic
        # Applicable to only python file.
        if not utils.is_py_file(file_name):
            _LOG.debug("Skipping file_name='%s'", file_name)
            return []

        output: List[str] = []

        output.extend(_check_file_path(file_name))

        # Read file.
        txt = io_.from_file(file_name).split("\n")
        # Process file.
        txt_new = _modify_file_lines(lines=txt)
        txt_new = _modify_file_line_by_line(lines=txt_new)
        output.extend(_check_file_lines(file_name=file_name, lines=txt_new))
        output.extend(
            _check_file_line_by_line(file_name=file_name, lines=txt_new)
        )
        # Write.
        utils.write_file_back(file_name, txt, txt_new)

        return output


# #############################################################################


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "files", nargs="+", action="store", type=str, help="Files to process",
    )
    prsr.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level)
    action = _P1SpecificLints()
    lntr.run_action(action, args.files)


if __name__ == "__main__":
    _main(_parse())
