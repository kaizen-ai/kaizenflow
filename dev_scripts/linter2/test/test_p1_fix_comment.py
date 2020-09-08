import pytest

import dev_scripts.linter2.p1_fix_comments as f_comm
import helpers.unit_test as hut


class Test_fix_comment_style(hut.TestCase):
    def test1(self) -> None:
        """Test no changes are applied to non-comments.

        - Given line is not a comment
        - When function runs
        - Then line is not changed
        """
        lines = ["test.method()"]
        actual = f_comm._fix_comment_style(lines)
        self.assertEqual(lines, actual)

    def test2(self) -> None:
        """Test first letter is capitalized.

        - Given comment starts with small letter
        - When function runs
        - Then comment starts with a capital letter
        """
        lines = ["# do this."]
        expected = ["# Do this."]

        actual = f_comm._fix_comment_style(lines)
        self.assertEqual(expected, actual)

    def test3(self) -> None:
        """Test comment is ended with a `.`

        - Given comment doesn't end with .
        - When function runs
        - Then comment ends with .
        """
        lines = ["# Do this"]
        expected = ["# Do this."]

        actual = f_comm._fix_comment_style(lines)
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

        actual = f_comm._fix_comment_style(lines)
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

        actual = f_comm._fix_comment_style(lines)
        self.assertEqual(expected, actual)

    def test6(self) -> None:
        """Test shebang lines are not changed.

        - Given shebang line
        - When function runs
        - Then line is not updated
        """
        lines = expected = ["#!/usr/bin/env python"]

        actual = f_comm._fix_comment_style(lines)
        self.assertEqual(expected, actual)

    def test7(self) -> None:
        """Test strings are not changed.

        - Given comment inside a string
        - When function runs
        - Then line is not updated
        """
        lines = expected = [r'comment_regex = r"(.*)#\s*(.*)\s*"']

        actual = f_comm._fix_comment_style(lines)
        self.assertEqual(expected, actual)

    def test8(self) -> None:
        """Test strings are not changed.

        - Given comment inside a string
        - When function runs
        - Then line is not updated
        """
        lines = expected = ['line = f"{match.group(1)}# {comment}"']

        actual = f_comm._fix_comment_style(lines)
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

        actual = f_comm._fix_comment_style(lines)
        self.assertEqual(expected, actual)

    def test10(self) -> None:
        """Test no changes are applied to empty comments.

        - Given line is an empty comment
        - When function runs
        - Then line is not changed
        """
        lines = ["#"]
        actual = f_comm._fix_comment_style(lines)
        self.assertEqual(lines, actual)

    def test11(self) -> None:
        """Test no changes are applied to comments that end in punctation.

        - Given line is a comment that ends with ?
        - When function runs
        - Then line is not changed
        """
        lines = ["# TODO(test): Should this be changed?"]
        actual = f_comm._fix_comment_style(lines)
        self.assertEqual(lines, actual)

    def test12(self) -> None:
        """Test no changes are applied to comments that start in a number.

        - Given line that starts in a number
        - When function runs
        - Then line is not changed
        """
        lines = ["# -1 is interpreted by joblib like for all cores."]
        actual = f_comm._fix_comment_style(lines)
        self.assertEqual(lines, actual)

    def test13(self) -> None:
        """Test no changes are applied to comments that start with '##'.

        - Given line that starts with '##'
        - When function runs
        - Then line is not changed
        """
        lines = ["## iNVALD"]
        actual = f_comm._fix_comment_style(lines)
        self.assertEqual(lines, actual)

    def test14(self) -> None:
        """Test no changes are applied to comments that start with 'pylint'."""
        lines = ["# pylint: disable=unused-argument"]
        actual = f_comm._fix_comment_style(lines)
        self.assertEqual(lines, actual)

    def test15(self) -> None:
        """Test no changes are applied to comments that start with 'type'."""
        lines = ["# type: noqa"]
        actual = f_comm._fix_comment_style(lines)
        self.assertEqual(lines, actual)

    def test16(self) -> None:
        """Test no changes are applied to comments with one word."""
        lines = expected = ["# oneword"]
        actual = f_comm._fix_comment_style(lines)
        self.assertEqual(expected, actual)

    def test17(self) -> None:
        """Test no changes are applied to comments with urls."""
        lines = expected = [
            ["# https://github.com/"],
            ["# https://google.com/"],
            ["# reference: https://facebook.com"],
        ]
        for line, e in zip(lines, expected):
            actual = f_comm._fix_comment_style(line)
            self.assertEqual(e, actual)

    def test18(self) -> None:
        """Test no changes are applied to comments that are valid python
        statements."""
        lines = expected = ["# print('hello')"]

        actual = f_comm._fix_comment_style(lines)
        self.assertEqual(expected, actual)

    def test19(self) -> None:
        lines = expected = [
            "# We need a matrix `c` for which `c*c^T = r`.",
            "# We can use # the Cholesky decomposition, or the we can construct `c`",
            "# from the eigenvectors and eigenvalues.",
            "# Compute the eigenvalues and eigenvectors.",
        ]

        actual = f_comm._fix_comment_style(lines)
        self.assertEqual(expected, actual)


class Test_extract_comments(hut.TestCase):
    def test1(self) -> None:
        """Test multi-line comments extracted successfully."""
        content = """
        # comment one
        # comment two
        """
        expected = [
            f_comm._LinesWithComment(
                start_line=2,
                end_line=3,
                multi_line_comment=[
                    "        # comment one",
                    "        # comment two",
                ],
            )
        ]
        actual = f_comm._extract_comments(content.split("\n"))
        self.assertEqual(expected, actual)

    def test2(self) -> None:
        """Test single line comments extracted successfully."""
        content = """
        # comment one
        """
        expected = [
            f_comm._LinesWithComment(
                start_line=2,
                end_line=2,
                multi_line_comment=["        # comment one"],
            )
        ]
        actual = f_comm._extract_comments(content.split("\n"))
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
            f_comm._LinesWithComment(
                start_line=2,
                end_line=3,
                multi_line_comment=[
                    "        # comment one",
                    "        # comment two",
                ],
            ),
            f_comm._LinesWithComment(
                start_line=5,
                end_line=5,
                multi_line_comment=["        # comment three"],
            ),
        ]
        actual = f_comm._extract_comments(content.split("\n"))
        self.assertEqual(expected, actual)


class Test_reflow_comment(hut.TestCase):
    def test1(self) -> None:
        """Test long comment is updated."""
        long_line = (
            "# This is a super long message that has too much information in it. "
            "Although inline comments are cool, this sentence should not be this long."
        )
        comment = f_comm._LinesWithComment(
            start_line=1, end_line=1, multi_line_comment=[long_line],
        )
        expected = f_comm._LinesWithComment(
            start_line=comment.start_line,
            end_line=comment.end_line,
            multi_line_comment=[
                "# This is a super long message that has too much information in it. Although",
                "# inline comments are cool, this sentence should not be this long.",
            ],
        )
        actual = f_comm._reflow_comment(comment)
        self.assertEqual(expected, actual)

    def test2(self) -> None:
        """Test markdown lists are respected."""
        comment = f_comm._LinesWithComment(
            start_line=1,
            end_line=2,
            multi_line_comment=["# - Hello", "# - How are you?"],
        )
        expected = comment
        actual = f_comm._reflow_comment(comment)
        self.assertEqual(expected, actual)

    def test3(self) -> None:
        """Test indentation is preserved."""
        comment = f_comm._LinesWithComment(
            start_line=1, end_line=1, multi_line_comment=["    # indented"],
        )
        expected = comment
        actual = f_comm._reflow_comment(comment)
        self.assertEqual(expected, actual)

    def test4(self) -> None:
        """Test a single comment with inconsistent whitespace raises error."""
        comment = f_comm._LinesWithComment(
            start_line=1,
            end_line=2,
            multi_line_comment=["# - Hello", "    # - How are you?"],
        )
        with self.assertRaises(AssertionError):
            f_comm._reflow_comment(comment)


class Test_replace_comments_in_lines(hut.TestCase):
    def test1(self) -> None:
        """Test replace comments in lines."""
        code_line = "method.test()"
        old_comment = "# old comment"
        new_comment = "# new comment"

        lines = [code_line, old_comment]
        updated_comments = [
            f_comm._LinesWithComment(
                start_line=2, end_line=2, multi_line_comment=[new_comment]
            )
        ]

        expected = [code_line, new_comment]
        actual = f_comm._replace_comments_in_lines(
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
        actual = f_comm._reflow_comments_in_lines(lines=content)
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
        result = f_comm._reflow_comments_in_lines(original)
        self.assertEqual(result, expected)
