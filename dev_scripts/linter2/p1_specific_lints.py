#!/usr/bin/env python
r"""Perform some p1 specific lints on python files

> p1_specific_lints.py sample_file1.py sample_file2.py
"""
import argparse
import ast
import dataclasses
import io
import logging
import os
import re
import string
import tempfile
import tokenize
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
class LinesWithComment:
    start_line: int
    end_line: int
    multi_line_comment: List[str]

    @property
    def is_single_line(self) -> bool:
        return len(self.multi_line_comment) == 1


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


def _is_valid_python_statement(comments: List[str]) -> bool:
    joined = "\n".join(comments)
    try:
        ast.parse(joined)
    except SyntaxError:
        return False
    return True


def _capitalize(comment: str) -> str:
    return f"{comment[0:2].upper()}{comment[2::]}"


def _fix_comment_style(lines: List[str]) -> List[str]:
    """Update comments to start with a capital letter and end with a `.`

    ignores:
    - empty line comments
    - comments that start with '##'
    - pylint & mypy comments
    - valid python statements
    """
    checks = (
        lambda x: x.startswith("##"),
        lambda x: x.startswith("# pylint"),
        lambda x: x.startswith("# type"),
        lambda x: x.startswith("#!"),
        lambda x: len(x.split()) == 2 and x.startswith("# "),
        lambda x: any(
            [
                re.match(
                    r"https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\."
                    r"[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)",
                    word,
                )
                is not None
                for word in x.split()
            ]
        ),
    )

    comments: List[LinesWithComment] = _extract_comments(lines)

    for comment in comments:
        if not comment.is_single_line:
            continue
        # If any of the checks returns True, it means the check failed.
        if any([check(comment.multi_line_comment[0]) for check in checks]):
            continue
        match = _parse_comment(comment.multi_line_comment[0], r"(^\s*)#(\s*)(.*)")
        if not match:
            continue
        without_pound = match.group(3)
        # Make sure it doesn't try to capitalize an empty comment
        if without_pound and not without_pound[0].isupper():
            without_pound = without_pound.capitalize()
        # Rebuild the comment and add punctuation if not already present
        body = f"{match.group(1)}#{match.group(2)}{without_pound}"
        if body[-1] not in string.punctuation:
            body = f"{body}."
        comment.multi_line_comment[0] = body

    return _replace_comments_in_lines(lines, comments)


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
