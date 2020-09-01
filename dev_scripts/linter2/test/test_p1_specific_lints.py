import pytest

import dev_scripts.linter2.p1_specific_lints as pslints
import helpers.unit_test as hut


class Test_fix_comment_style(hut.TestCase):
    def test1(self) -> None:
        """Test no changes are applied to non-comments.

        - Given line is not a comment
        - When function runs
        - Then line is not changed
        """
        lines = ["test.method()"]
        actual = pslints._fix_comment_style(lines)
        self.assertEqual(lines, actual)

    def test2(self) -> None:
        """Test first letter is capitalized.

        - Given comment starts with small letter
        - When function runs
        - Then comment starts with a capital letter
        """
        lines = ["# do this."]
        expected = ["# Do this."]

        actual = pslints._fix_comment_style(lines)
        self.assertEqual(expected, actual)

    def test3(self) -> None:
        """Test comment is ended with a `.`

        - Given comment doesn't end with .
        - When function runs
        - Then comment ends with .
        """
        lines = ["# Do this"]
        expected = ["# Do this."]

        actual = pslints._fix_comment_style(lines)
        self.assertEqual(expected, actual)

    @pytest.mark.skip(
        reason="""Inline comments are not allowed, as they are hard to maintain
        ref: https://github.com/ParticleDev/external/pull/16#discussion_r453418368
        """
    )
    def test4(self) -> None:
        """Test inline comments are processed.

        - Given line with code and a comment
        - And code doesn't end with .
        - When function runs
        - Then code is not changed
        - And comment ends with .
        """
        lines = ["test.method() # do this"]
        expected = ["test.method() # Do this."]

        actual = pslints._fix_comment_style(lines)
        self.assertEqual(expected, actual)

    def test5(self) -> None:
        """Test spaces are not updated.

        - Given line with a comment that doesn't start with a space
        - And lint has no trailing .
        - When function runs
        - Then line has a trailing .
        - And comment doesn't start with a space
        """
        lines = ["#Do this"]
        expected = ["#Do this."]

        actual = pslints._fix_comment_style(lines)
        self.assertEqual(expected, actual)

    def test6(self) -> None:
        """Test shebang lines are not changed.

        - Given shebang line
        - When function runs
        - Then line is not updated
        """
        lines = expected = ["#!/usr/bin/env python"]

        actual = pslints._fix_comment_style(lines)
        self.assertEqual(expected, actual)

    def test7(self) -> None:
        """Test strings are not changed.

        - Given comment inside a string
        - When function runs
        - Then line is not updated
        """
        lines = expected = [r'comment_regex = r"(.*)#\s*(.*)\s*"']

        actual = pslints._fix_comment_style(lines)
        self.assertEqual(expected, actual)

    def test8(self) -> None:
        """Test strings are not changed.

        - Given comment inside a string
        - When function runs
        - Then line is not updated
        """
        lines = expected = ['line = f"{match.group(1)}# {comment}"']

        actual = pslints._fix_comment_style(lines)
        self.assertEqual(expected, actual)

    def test9(self) -> None:
        """Test seperator lines are not changed.

        - Given seperator line
        - When function runs
        - Then line is not updated
        """
        lines = expected = [
            "# #############################################################################"
        ]

        actual = pslints._fix_comment_style(lines)
        self.assertEqual(expected, actual)

    def test10(self) -> None:
        """Test no changes are applied to empty comments.

        - Given line is an empty comment
        - When function runs
        - Then line is not changed
        """
        lines = ["#"]
        actual = pslints._fix_comment_style(lines)
        self.assertEqual(lines, actual)

    def test11(self) -> None:
        """Test no changes are applied to comments that end in punctation.

        - Given line is a comment that ends with ?
        - When function runs
        - Then line is not changed
        """
        lines = ["# TODO(test): Should this be changed?"]
        actual = pslints._fix_comment_style(lines)
        self.assertEqual(lines, actual)

    def test12(self) -> None:
        """Test no changes are applied to comments that start in a number.

        - Given line that starts in a number
        - When function runs
        - Then line is not changed
        """
        lines = ["# -1 is interpreted by joblib like for all cores."]
        actual = pslints._fix_comment_style(lines)
        self.assertEqual(lines, actual)

    def test13(self) -> None:
        """Test no changes are applied to comments that start with '##'.

        - Given line that starts with '##'
        - When function runs
        - Then line is not changed
        """
        lines = ["## iNVALD"]
        actual = pslints._fix_comment_style(lines)
        self.assertEqual(lines, actual)

    def test14(self) -> None:
        """Test no changes are applied to comments that start with 'pylint'."""
        lines = ["# pylint: disable=unused-argument"]
        actual = pslints._fix_comment_style(lines)
        self.assertEqual(lines, actual)

    def test15(self) -> None:
        """Test no changes are applied to comments that start with 'type'."""
        lines = ["# type: noqa"]
        actual = pslints._fix_comment_style(lines)
        self.assertEqual(lines, actual)

    def test16(self) -> None:
        """Test no changes are applied to comments with one word."""
        lines = expected = ["# oneword"]
        actual = pslints._fix_comment_style(lines)
        self.assertEqual(expected, actual)

    def test17(self) -> None:
        """Test no changes are applied to comments with urls."""
        lines = expected = [
            ["# https://github.com/"],
            ["# https://google.com/"],
            ["# reference: https://facebook.com"],
        ]
        for l, e in zip(lines, expected):
            actual = pslints._fix_comment_style(l)
            self.assertEqual(e, actual)

    def test18(self) -> None:
        """Test no changes are applied to comments that are valid python
        statements."""
        lines = expected = ["# print('hello')"]

        actual = pslints._fix_comment_style(lines)
        self.assertEqual(expected, actual)

    def test19(self) -> None:
        lines = expected = [
            "# We need a matrix `c` for which `c*c^T = r`.",
            "# We can use # the Cholesky decomposition, or the we can construct `c`",
            "# from the eigenvectors and eigenvalues.",
            "# Compute the eigenvalues and eigenvectors.",
        ]

        actual = pslints._fix_comment_style(lines)
        self.assertEqual(expected, actual)


class Test_warn_incorrectly_formatted_todo(hut.TestCase):
    def test1(self) -> None:
        """Test warning for missing assignee.

        - Given line has incorrectly formatted todo in comment
        - When function runs
        - Then a warning is returned
        """
        line = "# todo: invalid"

        msg = pslints._warn_incorrectly_formatted_todo("test.py", 1, line)
        self.assertIn("test.py:1: found incorrectly formatted TODO comment", msg)

    def test2(self) -> None:
        """Test no warning for no todo comment.

        - Given line has no todo comment
        - When function runs
        - Then no warning is returned
        """
        line = "test.method()"

        msg = pslints._warn_incorrectly_formatted_todo("test.py", 1, line)
        self.assertEqual("", msg)

    def test3(self) -> None:
        """Test no warning for code line with `todo` in variable name.

        - Given line has todo in variable name
        - When function runs
        - Then no warning is returned
        """
        line = "todo_var = 3"

        msg = pslints._warn_incorrectly_formatted_todo("test.py", 1, line)
        self.assertEqual("", msg)

    def test4(self) -> None:
        """Test warning for no assignee.

        - Given todo comment has no assignee
        - When function runs
        - Then a warning is returned
        """
        line = "# TODO: hi"

        msg = pslints._warn_incorrectly_formatted_todo("test.py", 1, line)
        self.assertIn("test.py:1: found incorrectly formatted TODO comment", msg)

    def test5(self) -> None:
        """Test no warning for valid todo.

        - Given todo comment is valid
        - When function runs
        - Then no warning is returned
        """
        line = "# TODO(test): hi"

        msg = pslints._warn_incorrectly_formatted_todo("test.py", 1, line)
        self.assertEqual("", msg)

    def test6(self) -> None:
        """Test no warning for missing trailing `.`. The check for trailing
        punctation is covered by `_fix_comment_style`.

        - Given todo comment is missing a trailing .
        - When function runs
        - Then no warning is returned
        """
        line = "# TODO(test): hi"

        msg = pslints._warn_incorrectly_formatted_todo("test.py", 1, line)
        self.assertEqual("", msg)

    def test7(self) -> None:
        """Test no warning for a comment that has 'todo' as a substring.

        - Given a comment that has a 'todo' as a substring but isn't a todo comment
        - When function runs
        - Then no warning is returned
        """
        line = "# The line is not a todo comment"

        msg = pslints._warn_incorrectly_formatted_todo("test.py", 1, line)
        self.assertEqual("", msg)

    def test8(self) -> None:
        """Test no warning for a comment that has todo in a string.

        - Given invalid todo comment that has a warning rule
        - When function runs
        - Then no warning is returned
        """
        line = 'line = "# TODO(test): hi"'

        msg = pslints._warn_incorrectly_formatted_todo("test.py", 1, line)
        self.assertEqual("", msg)


class Test_check_notebook_dir(hut.TestCase):
    def test_check_notebook_dir1(self) -> None:
        """The notebook is not under 'notebooks': invalid."""
        file_name = "hello/world/notebook.ipynb"
        exp = (
            "hello/world/notebook.ipynb:1: "
            "each notebook should be under a 'notebooks' directory to not confuse pytest"
        )
        self._helper_check_notebook_dir(file_name, exp)

    def test_check_notebook_dir2(self) -> None:
        """The notebook is under 'notebooks': valid."""
        file_name = "hello/world/notebooks/notebook.ipynb"
        exp = ""
        self._helper_check_notebook_dir(file_name, exp)

    def test_check_notebook_dir3(self) -> None:
        """It's not a notebook: valid."""
        file_name = "hello/world/notebook.py"
        exp = ""
        self._helper_check_notebook_dir(file_name, exp)

    def _helper_check_notebook_dir(self, file_name: str, exp: str) -> None:
        msg = pslints._check_notebook_dir(file_name)
        self.assert_equal(msg, exp)


class Test_check_test_file_dir(hut.TestCase):
    def test_check_test_file_dir1(self) -> None:
        """Test is under `test`: valid."""
        file_name = "hello/world/test/test_all.py"
        exp = ""
        self._helper_check_test_file_dir(file_name, exp)

    def test_check_test_file_dir2(self) -> None:
        """Test is not under `test`: invalid."""
        file_name = "hello/world/test_all.py"
        exp = (
            "hello/world/test_all.py:1: "
            "test files should be under 'test' directory to be discovered by pytest"
        )
        self._helper_check_test_file_dir(file_name, exp)

    def test_check_test_file_dir3(self) -> None:
        """Test is not under `test`: invalid."""
        file_name = "hello/world/tests/test_all.py"
        exp = (
            "hello/world/tests/test_all.py:1: "
            "test files should be under 'test' directory to be discovered by pytest"
        )
        self._helper_check_test_file_dir(file_name, exp)

    def test_check_test_file_dir4(self) -> None:
        """It's a notebook: valid."""
        file_name = "hello/world/tests/test_all.ipynb"
        exp = ""
        self._helper_check_test_file_dir(file_name, exp)

    def _helper_check_test_file_dir(self, file_name: str, exp: str) -> None:
        msg = pslints._check_test_file_dir(file_name)
        self.assert_equal(msg, exp)


class Test_check_import(hut.TestCase):
    def test1(self) -> None:
        """Test long import shortcut: invalid."""
        shortcut = "very_long_name"
        line = f"import test as {shortcut}"
        exp = f"the import shortcut '{shortcut}' in '{line}' is longer than 8 characters"
        self._helper_check_import(line, exp, file_name="test.py")

    def test2(self) -> None:
        """Test from lib import something: invalid."""
        line = "from pandas import DataFrame"
        exp = f"do not use '{line}' use 'import foo.bar " "as fba'"
        self._helper_check_import(line, exp, file_name="test.py")

    def test3(self) -> None:
        """Test from typing import something: valid."""
        line = "from typing import List"
        exp = ""
        self._helper_check_import(line, exp, file_name="test.py")

    def test4(self) -> None:
        """Test wild import in __init__.py: valid."""
        line = "from test import *"
        exp = ""
        self._helper_check_import(line, exp, file_name="__init__.py")

    def test5(self) -> None:
        """Test import test.ab as tab: valid."""
        line = "import test.ab as tab"
        exp = ""
        self._helper_check_import(line, exp, file_name="test.py")

    def _helper_check_import(self, line: str, exp: str, file_name: str) -> None:
        file_name = file_name or "test.py"
        line_num = 1
        exp = f"{file_name}:{line_num}: {exp}" if exp else exp
        msg = pslints._check_import(file_name, line_num, line)
        self.assertEqual(exp, msg)


class Test_format_separating_lines(hut.TestCase):
    def test1(self) -> None:
        """Test seperator lines are formatted correctly."""
        min_num_chars = 6
        line_width = 78

        line = f"# {'#' * min_num_chars}"
        exp = f"# {'#' * (line_width - 1)}"
        actual = pslints._format_separating_line(
            line, min_num_chars=min_num_chars, line_width=line_width
        )
        self.assertEqual(exp, actual)

    def test2(self) -> None:
        """Test lines that don't meet the min number of chars aren't
        updated."""
        min_num_chars = 10

        line = f"# {'#' * (min_num_chars - 1)}"
        exp = line
        actual = pslints._format_separating_line(
            line, min_num_chars=min_num_chars,
        )
        self.assertEqual(exp, actual)

    def test3(self) -> None:
        """Test seperator lines can use different charachters."""
        min_num_chars = 6
        line_width = 78

        line = f"# {'=' * min_num_chars}"
        exp = f"# {'=' * (line_width - 1)}"
        actual = pslints._format_separating_line(
            line, min_num_chars=min_num_chars, line_width=line_width
        )
        self.assertEqual(exp, actual)

    def test4(self) -> None:
        """Check that it doesn't replace if the bar is not until the end of the
        line."""
        min_num_chars = 6
        line_width = 78

        line = f"# {'=' * min_num_chars} '''"
        exp = line
        actual = pslints._format_separating_line(
            line, min_num_chars=min_num_chars, line_width=line_width
        )
        self.assertEqual(exp, actual)


class Test_check_notebook_filename(hut.TestCase):
    def test1(self) -> None:
        r"""Check python files are not checked

        - Given python file
        - When function runs
        - Then no warning message is returned"""
        file_name = "linter/module.py"
        actual = pslints._check_notebook_filename(file_name)
        self.assertEqual("", actual)

    def test2(self) -> None:
        r"""Check filename rules

        - Given notebook filename starts with `Master_`
        - When function runs
        - Then no warning message is returned"""
        file_name = "linter/Master_notebook.ipynb"
        actual = pslints._check_notebook_filename(file_name)
        self.assertEqual("", actual)

    def test3(self) -> None:
        r"""Check filename rules

        - Given notebook filename matchs `\S+Task\d+_...`
        - When function runs
        - Then no warning message is returned"""
        file_name = "linter/PartTask400_test.ipynb"
        actual = pslints._check_notebook_filename(file_name)
        self.assertEqual("", actual)

    def test4(self) -> None:
        r"""Check filename rules

        - Given notebook filename doesn't start with `Master_`
        - And notebook filename doesn't match `\S+Task\d+_...`
        - When function runs
        - Then a warning message is returned"""
        file_name = "linter/notebook.ipynb"
        exp = (
            f"{file_name}:1: "
            r"All notebook filenames start with `Master_` or match: `\S+Task\d+_...`"
        )
        actual = pslints._check_notebook_filename(file_name)
        self.assertEqual(exp, actual)

    def test5(self) -> None:
        r"""Check filename rules

        - Given notebook filename doesn't start with `Master_`
        - And notebook filename doesn't match `\S+Task\d+_...`
        - When function runs
        - Then a warning message is returned"""
        file_name = "linter/Task400.ipynb"
        exp = (
            f"{file_name}:1: "
            r"All notebook filenames start with `Master_` or match: `\S+Task\d+_...`"
        )
        actual = pslints._check_notebook_filename(file_name)
        self.assertEqual(exp, actual)

    def test6(self) -> None:
        r"""Check filename rules

        - Given notebook filename doesn't start with `Master_`
        - And notebook filename doesn't match `\S+Task\d+_...`
        - When function runs
        - Then a warning message is returned"""
        file_name = "linter/MegaTask200.ipynb"
        exp = (
            f"{file_name}:1: "
            r"All notebook filenames start with `Master_` or match: `\S+Task\d+_...`"
        )
        actual = pslints._check_notebook_filename(file_name)
        self.assertEqual(exp, actual)


class Test_extract_comments(hut.TestCase):
    def test1(self) -> None:
        """Test multi-line comments extracted successfully."""
        content = """
        # comment one
        # comment two
        """
        expected = [
            pslints.LinesWithComment(
                start_line=2,
                end_line=3,
                multi_line_comment=[
                    "        # comment one",
                    "        # comment two",
                ],
            )
        ]
        actual = pslints._extract_comments(content.split("\n"))
        self.assertEqual(expected, actual)

    def test2(self) -> None:
        """Test single line comments extracted successfully."""
        content = """
        # comment one
        """
        expected = [
            pslints.LinesWithComment(
                start_line=2,
                end_line=2,
                multi_line_comment=["        # comment one"],
            )
        ]
        actual = pslints._extract_comments(content.split("\n"))
        self.assertEqual(expected, actual)

    def test3(self) -> None:
        """Test single & multi-line comments extracted successfully."""
        content = """
        # comment one
        # comment two
        test.method()
        # comment three
        """
        expected = [
            pslints.LinesWithComment(
                start_line=2,
                end_line=3,
                multi_line_comment=[
                    "        # comment one",
                    "        # comment two",
                ],
            ),
            pslints.LinesWithComment(
                start_line=5,
                end_line=5,
                multi_line_comment=["        # comment three"],
            ),
        ]
        actual = pslints._extract_comments(content.split("\n"))
        self.assertEqual(expected, actual)


class Test_reflow_comment(hut.TestCase):
    def test1(self) -> None:
        """Test long comment is updated."""
        long_line = (
            "# This is a super long message that has too much information in it. "
            "Although inline comments are cool, this sentence should not be this long."
        )
        comment = pslints.LinesWithComment(
            start_line=1, end_line=1, multi_line_comment=[long_line],
        )
        expected = pslints.LinesWithComment(
            start_line=comment.start_line,
            end_line=comment.end_line,
            multi_line_comment=[
                "# This is a super long message that has too much information in it. Although",
                "# inline comments are cool, this sentence should not be this long.",
            ],
        )
        actual = pslints._reflow_comment(comment)
        self.assertEqual(expected, actual)

    def test2(self) -> None:
        """Test markdown lists are respected."""
        comment = pslints.LinesWithComment(
            start_line=1,
            end_line=2,
            multi_line_comment=["# - Hello", "# - How are you?"],
        )
        expected = comment
        actual = pslints._reflow_comment(comment)
        self.assertEqual(expected, actual)

    def test3(self) -> None:
        """Test indentation is preserved."""
        comment = pslints.LinesWithComment(
            start_line=1, end_line=1, multi_line_comment=["    # indented"],
        )
        expected = comment
        actual = pslints._reflow_comment(comment)
        self.assertEqual(expected, actual)

    def test4(self) -> None:
        """Test a single comment with inconsistent whitespace raises error."""
        comment = pslints.LinesWithComment(
            start_line=1,
            end_line=2,
            multi_line_comment=["# - Hello", "    # - How are you?"],
        )
        with self.assertRaises(AssertionError):
            pslints._reflow_comment(comment)


class Test_replace_comments_in_lines(hut.TestCase):
    def test1(self) -> None:
        """Test replace comments in lines."""
        code_line = "method.test()"
        old_comment = "# old comment"
        new_comment = "# new comment"

        lines = [code_line, old_comment]
        updated_comments = [
            pslints.LinesWithComment(
                start_line=2, end_line=2, multi_line_comment=[new_comment]
            )
        ]

        expected = [code_line, new_comment]
        actual = pslints._replace_comments_in_lines(
            lines=lines, comments=updated_comments
        )
        self.assertEqual(expected, actual)


class _reflow_comments_in_lines(hut.TestCase):
    def test1(self) -> None:
        content = """
        test.method_before()
        # This is a super long message that has too much information in it. Although inline comments are cool, this sentence should not be this long.
        test.method_after()
        # - a list item
        # - another list item
        """.split(
            "\n"
        )
        expected = """
        test.method_before()
        # This is a super long message that has too much information in it. Although
        # inline comments are cool, this sentence should not be this long.
        test.method_after()
        # - a list item
        # - another list item
        """.split(
            "\n"
        )
        actual = pslints._reflow_comments_in_lines(lines=content)
        self.assertEqual(expected, actual)


class Test_reflow_comments(hut.TestCase):
    def test_1(self) -> None:
        """Test combination of too short and too long lines."""
        original = [
            "# Create decorated functions with different caches and store pointers of these "
            + "functions. Note that we need to build the functions in the constructor since we",
            "# need to have a single instance of the decorated"
            + " functions. On the other side,",
            "# e.g., if we created these functions in `__call__`, they will be recreated at "
            + "every invocation, creating a new memory cache at every invocation.",
        ]
        expected = [
            "# Create decorated functions with different caches and store pointers of these",
            "# functions. Note that we need to build the functions in the constructor since we",
            "# need to have a single instance of the decorated functions. On the other side,",
            "# e.g., if we created these functions in `__call__`, they will be recreated at",
            "# every invocation, creating a new memory cache at every invocation.",
        ]
        result = pslints._reflow_comments_in_lines(original)
        self.assertEqual(result, expected)
