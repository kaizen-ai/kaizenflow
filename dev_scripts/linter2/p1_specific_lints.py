#!/usr/bin/env python
r"""Perform some p1 specific lints on python files

> p1_specific_lints.py -f sample_file.py
"""
from __future__ import annotations
import argparse
import dataclasses
import io
import logging
import os
import re
import string
import tempfile
import tokenize
import enum
from typing import Callable, Dict, List, Optional, Tuple, Union

import more_itertools
import typing_extensions

import dev_scripts.linter2.base as lntr
import dev_scripts.linter2.utils as utils
import helpers.dbg as dbg
import helpers.io_ as io_
import helpers.parser as prsr

_LOG = logging.getLogger(__name__)

# #############################################################################
# Utilities.
# #############################################################################
# TODO(amr): Move to linter/utils.py


def _is_separator(line: str) -> bool:
    """Check if the line matches a separator line.

    :return: True if it matches a separator line
    """
    return (
        line
        == "# #############################################################################"
    )


def _is_shebang(line: str) -> bool:
    """Check if the line is a shebang (starts with #!)

    :return: True if it is a shebang (starts with #!)
    """
    return line.startswith("#!")


def _parse_comment(
    line: str, regex: str = r"(^\s*)#\s*(.*)\s*"
) -> Optional[re.Match]:
    """Parse a line and return a comment if there's one.

    Seperator lines and shebang return None.
    """
    if _is_separator(line) or _is_shebang(line):
        return None

    return re.search(regex, line)


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

        def __call__(self, file_name: str, lines: List[str]) -> str:
            # This definition is needed as typing.Callable doesn't support keyword arguments.
            # Ref: https://github.com/python/mypy/issues/1655.
            ...

    CONTENT_CHECKS: List[ContentCheck] = [
        _check_shebang,
    ]

    output: List[str] = []
    for check in CONTENT_CHECKS:
        msg = check(file_name=file_name, lines=lines)
        if msg:
            output.append(msg)

    return output


# #############################################################################
# File Content Modifiers.
# #############################################################################


@dataclasses.dataclass
class LinesWithComment:
    start_line: int
    end_line: int
    multi_line_comment: List[str]


# the key is the line number, the value is the comment.
Comment = Dict[int, str]


def _extract_comments(lines: List[str]) -> List[LinesWithComment]:
    """Extract comments (which can be single line or multi-lines) from a list
    of file lines, all consecutive lines with a comment would be merged into a
    single multiline comment."""
    content = "\n".join(lines)
    tokens = tokenize.tokenize(io.BytesIO(content.encode("utf-8")).readline)
    comments_by_line = {
        t.start[0]: t.line.rstrip() for t in tokens if t.type == tokenize.COMMENT
    }

    # find consecutive line numbers to determine multi-line comments
    comment_line_numbers = comments_by_line.keys()
    comments: List[LinesWithComment] = []
    for group in more_itertools.consecutive_groups(comment_line_numbers):
        line_numbers = list(group)
        # TODO(*): Do a single scan using an FSM to build this map.
        # Reference: https://github.com/ParticleDev/external/pull/65/files#r464000483
        matching_comments = [
            line
            for line_num, line in comments_by_line.items()
            if line_num in line_numbers
        ]
        comments.append(
            LinesWithComment(
                start_line=min(line_numbers),
                end_line=max(line_numbers),
                multi_line_comment=matching_comments,
            )
        )

    return comments


def _reflow_comment(comment: LinesWithComment) -> LinesWithComment:
    """Reflow comment using prettier."""
    content = ""
    whitespace: Optional[str] = None
    for line in comment.multi_line_comment:
        match = _parse_comment(line)
        if match is None:
            if not _is_shebang(line) and not _is_separator(line):
                _LOG.warning("'%s' doesn't have a comment!", line)
            return comment
        content += "\n" + match.group(2)

        # assumption: all consecutive comments have the same indentation
        if whitespace is None:
            whitespace = match.group(1)
        else:
            dbg.dassert_eq(whitespace, match.group(1))

    tmp = tempfile.NamedTemporaryFile(suffix=".md")
    io_.to_file(file_name=tmp.name, lines=content)

    cmd = f"prettier --prose-wrap always --write {tmp.name}"
    lntr.tee(cmd, "prettier", abort_on_error=False)
    content: str = io_.from_file(file_name=tmp.name)
    tmp.close()

    updated_multi_line_comment: List[str] = []
    for line in content.strip().split("\n"):
        updated_multi_line_comment.append(str(whitespace) + "# " + line)

    comment.multi_line_comment = updated_multi_line_comment
    return comment


def _replace_comments_in_lines(
    lines: List[str], comments: List[LinesWithComment]
) -> List[str]:
    """Replace comments in lines.

    - For each comment:
        1. finds the the index in lines where the new lines should be inserted
        2. removes the lines between the comment's start_line & end_line.
        3. adds the new multiline comment
    """
    LineWithNumber = Tuple[int, str]
    lines_with_numbers: List[LineWithNumber] = [
        (idx + 1, line) for idx, line in enumerate(lines)
    ]

    updated_lines_with_numbers = lines_with_numbers.copy()
    for comment in comments:
        # find index of first line that matches those line nums
        index_to_insert_at = next(
            idx
            for idx, (line_num, line) in enumerate(updated_lines_with_numbers)
            if line_num == comment.start_line
        )

        # remove lines that are not between start_line & end_line
        updated_lines_with_numbers = [
            (line_num, line)
            for line_num, line in updated_lines_with_numbers
            if line_num < comment.start_line or line_num > comment.end_line
        ]

        # insert the new lines at that index
        inserted_lines = [(-1, line) for line in comment.multi_line_comment]
        updated_lines_with_numbers = (
            updated_lines_with_numbers[:index_to_insert_at]
            + inserted_lines
            + updated_lines_with_numbers[index_to_insert_at:]
        )

    updated_lines = [line for line_num, line in updated_lines_with_numbers]
    return updated_lines


def _reflow_comments_in_lines(lines: List[str]) -> List[str]:
    comments = _extract_comments(lines=lines)
    reflowed_comments = [_reflow_comment(c) for c in comments]
    updated_lines = _replace_comments_in_lines(
        lines=lines, comments=reflowed_comments,
    )
    return updated_lines


class Node:
    class Type(enum.Enum):
        CLS = 1
        FUNC = 2
        TEXT = 3

    def __init__(self, name: str, line: str, type: Node.Type):
        """Represents either a class, function or raw text.
        :param name: the function or class name
        :param line: the complete line on which the node was found
        :param type: class, function or raw text
        """
        self.type = type
        self.children: List[Node] = list()
        self._line = line
        self._name = name
        self._body: List[str] = list()
        self._decorators: List[str] = list()
        self._indent = None

    @property
    def indentation(self) -> int:
        if self._indent is None:
            self._indent = len(self._line) - len(self._line.lstrip(' '))
        return self._indent

    def add_child(self, child: Node):
        self.children.append(child)

    def add_to_body(self, line: str):
        self._body.append(line)

    def add_decorators(self, decorators: List[str]):
        self._decorators.extend(decorators)

    def to_lines(self) -> List[str]:
        lines: List[str] = self._decorators + [self._line] + self._body
        for child in self.children:
            lines.extend(child.to_lines())
        return lines

    def _find_child_by_name(self, name: str):
        return next((node for node in self.children if node._name == name), None)

    @staticmethod
    def _sorting_key(item: Node) -> int:
        """Sorts in order of: classes > __init__ > magic methods > public methods > private methods."""
        if item._name == '__init__':
            return 1
        if item.type == Node.Type.CLS:
            return 0
        if item._name.startswith('__') and item._name.endswith('__'):
            return 2
        if item._name.startswith('_'):
            return 4
        return 3

    def order_children(self, force=None):
        if self.type != Node.Type.CLS and not force:
            _LOG.warning("You tried to order a node that isn't a class. This can possibly mess up the formatting. "
                         "Use the force param if you're sure.")
            return
        self.children = sorted(self.children, key=self._sorting_key)

    def __repr__(self):
        types_as_str = {
            Node.Type.FUNC: 'function',
            Node.Type.CLS: 'class',
            Node.Type.TEXT: 'misc'
        }
        return f"{types_as_str[self.type]} <{self._name}>"


def _node_from_line(line: str) -> Union[Node, str]:
    """Constructs a Node object from a line, if it declares a function or class.
    :param line: the line from which the object needs to be constructed
    :return: returns the line if no function/class was declared, else it returns a instantiated Node object
    """
    class_name: str = utils.is_class_declaration(line)
    function_name: str = utils.is_function_declaration(line)
    if class_name:
        new_node = Node(class_name, line, Node.Type.CLS)
    elif function_name:
        new_node = Node(function_name, line, Node.Type.FUNC)
    else:
        new_node = line
    return new_node


def _find_parent_node(nodes: List[Node], indent: int) -> Union[None, Node]:
    """Finds the parent node, based on level of indentation.

    We assume that classes can be the parents of functions and other classes and functions can be the parents of
    other functions and classes. Searches over the nodes and their children recursively.
    :param nodes: nodes to check on
    :param indent: level of indentation of the node for which the parent needs to be found
    :return: the parent node if found, else None
    """
    # only classes and functions can be parents
    filtered_nodes: List[Node] = [n for n in nodes if n.type in [Node.Type.CLS, Node.Type.FUNC]]
    if not filtered_nodes:
        return
    # the parent has to be the last from the list
    pn: Node = filtered_nodes[-1]
    if pn.indentation == indent - 4:
        return pn
    else:
        return _find_parent_node(pn.children, indent)


def _lines_to_nodes(lines: List[str]) -> List[Node]:
    """Creates a list of nodes from a list of lines.

    All nodes in the list, are the root (parentless?) nodes. Where appropiate, the root nodes have children.
    :param lines: the lines from which to construct the nodes
    :return: a list of nodes representing the lines
    """
    root_nodes: List[Node] = list()
    decorators: List[str] = list()
    last_node: Union[None, Node] = None
    parent_node: Union[None, Node] = None
    previous_indent: Union[None, int] = None
    next_line_is_decorator: Union[None, bool] = None
    for line in lines:
        # support for multi-line decorators
        is_decorator = utils.is_decorator(line)
        if (is_decorator and '(' in line) or next_line_is_decorator:
            decorators.append(line)
            if line.endswith('\n'):
                chars_to_reverse = -3
            else:
                chars_to_reverse = -1
            next_line_is_decorator = line[chars_to_reverse] != ')'
            continue
        # support for simple decorators, i.e. @staticmethod or @abstractmethod
        elif is_decorator:
            decorators.append(line)
            continue
        # Unless we're in the body of a function/class, it is safe to assume that the parent node has changed if the
        # level of indentation has moved.
        current_indent: int = len(line) - len(line.lstrip(' '))
        if current_indent != previous_indent:
            parent_node: Union[None, Node] = _find_parent_node(root_nodes, current_indent)
        #
        node: Union[Node, str] = _node_from_line(line)
        if isinstance(node, str):
            if last_node is not None:
                # all lines from within a class(basically only class variables) and all lines from within functions
                last_node.add_to_body(line)
            else:
                # create a misc node to make sure all lines before function/class nodes nodes (shebangs, imports,
                # etc) are also saved
                last_node = node = Node('', line, Node.Type.TEXT)
                root_nodes.append(node)
        elif isinstance(node, Node):
            if parent_node is None:
                # if there is no current parent node, the new node has to be the parent
                parent_node = node
            if decorators:
                node.add_decorators(decorators)
                decorators = list()
            if current_indent == 0:
                root_nodes.append(node)
            else:
                parent_node.add_child(node)
            #
            last_node = node
        previous_indent = current_indent
    return root_nodes


def _nodes_to_lines_in_correct_order(nodes: List[Node]) -> List[str]:
    updated_lines: List[str] = list()
    for node in nodes:
        if node.type == Node.Type.CLS:
            node.order_children()
        updated_lines.extend(node.to_lines())
    return updated_lines


def _correct_order_of_methods(lines: List[str]) -> List[str]:
    root_nodes = _lines_to_nodes(lines)
    updated_lines = _nodes_to_lines_in_correct_order(root_nodes)
    return updated_lines


def _modify_file_lines(lines: List[str]) -> List[str]:
    """Modify multiple lines based on some rules, returns an updated list of
    line.

    :param lines: lines of content
    :return: updated lines of content
    """
    # Functions that take a line, and return a modified line.
    ContentModifier = Callable[[List[str]], List[str]]
    CONTENT_MODIFIERS: List[ContentModifier] = [
        # TODO(amr): enable this once it's tested enough.
        # _reflow_comments_in_lines
    ]

    for modifier in CONTENT_MODIFIERS:
        lines = modifier(lines)
    return lines


# #############################################################################
# File Line Modifiers.
# #############################################################################


def _fix_comment_style(line: str) -> str:
    """Update comments to start with a capital letter and end with a `.`

    ignores:
    - empty line comments
    - comments that start with '##'

    :param line: text line
    :return: modified line
    """
    excluded_prefixes = (
        # Ignore comments that start with '##'
        "#",
        # Ignore special comments.
        "pylint",
        "type",
    )

    new_line = line
    match = _parse_comment(line=line, regex=r"(^\s*)#(\s*)(.*)")
    if match:
        comment = match.group(3)
        if comment and not any(
            [comment.startswith(prefix) for prefix in excluded_prefixes]
        ):
            # Capitalize the first letter.
            comment = comment[0].capitalize() + comment[1:]
            # Make sure it ends with a . if it doesn't end with punctuation.
            if comment[-1] not in string.punctuation:
                comment += "."
            new_line = f"{match.group(1)}#{match.group(2)}{comment}"
    return new_line


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
        # TODO(amr): re-enable this for multi-line comments only.
        # _fix_comment_style,
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

    match = _parse_comment(line=line)
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


def _check_import(file_name: str, line_num: int, line: str) -> str:
    # The maximum length of an 'import as'.
    MAX_LEN_IMPORT = 8

    msg = ""

    if utils.is_init_py(file_name):
        # In __init__.py we can import in weird ways.
        # (e.g., the evil `from ... import *`).
        return msg

    m = re.match(r"\s*from\s+(\S+)\s+import\s+.*", line)
    if m:
        if m.group(1) != "typing":
            msg = "%s:%s: do not use '%s' use 'import foo.bar " "as fba'" % (
                file_name,
                line_num,
                line.rstrip().lstrip(),
            )
    else:
        m = re.match(r"\s*import\s+\S+\s+as\s+(\S+)", line)
        if m:
            shortcut = m.group(1)
            if len(shortcut) > MAX_LEN_IMPORT:
                msg = (
                    "%s:%s: the import shortcut '%s' in '%s' is longer than "
                    "%s characters"
                    % (
                        file_name,
                        line_num,
                        shortcut,
                        line.rstrip().lstrip(),
                        MAX_LEN_IMPORT,
                    )
                )
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
        _check_import,
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