from typing import List

import helpers.hstring as hstring
import helpers.hunit_test as hunitest


class TestGetDocstringLineIndices(hunitest.TestCase):
    """
    Test determining which code lines are inside (doc)strings.
    """

    def helper(self, code: str, expected: List[str]) -> None:
        lines = code.split("\n")
        actual_idxs = hstring.get_docstring_line_indices(lines)
        actual = [lines[i].strip() for i in actual_idxs]
        self.assertEqual(actual, expected)

    def test1(self) -> None:
        """
        Test one type of quotes.
        """
        code = """
class TestNewCase(hunitest.TestCase):
    def test_assert_equal1(self) -> None:
        '''
        Test one.
        '''
        actual = "hello world"
        expected = actual
        self.assert_equal(actual, expected)

    def test_check_string1(self) -> None:
        actual = "hello world"
        s = '''
        Inside a string.
        '''
        d = '''Does not count'''
        self.check_string(actual)

        """
        expected = ["'''", "Test one.", "s = '''", "Inside a string."]
        self.helper(code, expected)

    def test2(self) -> None:
        """
        Test the second type of quotes.
        """
        code = '''
class TestNewCase(hunitest.TestCase):
    def test_assert_equal1(self) -> None:
        """
        Test one.
        """
        actual = "hello world"
        expected = actual
        self.assert_equal(actual, expected)

    def test_check_string1(self) -> None:
        actual = "hello world"
        s = """
        Inside a string.
        """
        d = """Does not count"""
        self.check_string(actual)

        '''
        expected = ['"""', "Test one.", 's = """', "Inside a string."]
        self.helper(code, expected)

    def test3(self) -> None:
        """
        Test quotes within quotes.
        """
        code = """
class TestNewCase(hunitest.TestCase):
    def test_assert_equal1(self) -> None:
        '''
        Test one.
        """
        code += '''\
"""
        String within "Test one".
        """
        '''
        code += """\
'''
        actual = "hello world"
        expected = actual
        self.assert_equal(actual, expected)

    def test_check_string1(self) -> None:
        actual = "hello world"
        s = '''
        Inside a string.
        '''
        d = '''Does not count'''
        self.check_string(actual)

        """
        expected = [
            "'''",
            "Test one.",
            '"""',
            'String within "Test one".',
            '"""',
            "s = '''",
            "Inside a string.",
        ]
        self.helper(code, expected)