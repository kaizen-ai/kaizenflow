from typing import List

import pytest

import dev_scripts.linter2.p1_specific_lints as pslints
import helpers.unit_test as hut


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


class Test_correct_method_order(hut.TestCase):
    @property
    def correct_lines(self) -> List[str]:
        return [
            "import math",
            "class Mather:",
            "    def __init__(c1: int, c2: int):",
            "        self._comb = math.comb(c1, c2)",
            "",
            "    def shout(self):",
            "        print(f'IT IS {self._comb}')",
            "",
            "    def _whisper(self):",
            "        print('it is {self._comb}')",
            "",
        ]

    @property
    def correct_lines_with_decorator(self) -> List[str]:
        tmp = self.correct_lines.copy()
        tmp.insert(8, "    @staticmethod")
        return tmp

    @property
    def correct_lines_with_multiline_decorator(self) -> List[str]:
        tmp = self.correct_lines.copy()
        tmp.insert(8, "    @staticmethod(")
        tmp.insert(9, "       arg=1,")
        tmp.insert(10, "       param=2)")
        return tmp

    def test1(self) -> None:
        """Test conversion between nodes and lines with no changes to be
        made."""
        correct_lines_nodes = pslints._lines_to_nodes(self.correct_lines)
        self.assertEqual(
            pslints._nodes_to_lines_in_correct_order(correct_lines_nodes),
            self.correct_lines,
        )
        correct_lines_decorator = pslints._lines_to_nodes(
            self.correct_lines_with_decorator
        )
        self.assertEqual(
            pslints._nodes_to_lines_in_correct_order(correct_lines_decorator),
            self.correct_lines_with_decorator,
        )
        correct_lines_multiline_decorator = pslints._lines_to_nodes(
            self.correct_lines_with_multiline_decorator
        )
        self.assertEqual(
            pslints._nodes_to_lines_in_correct_order(
                correct_lines_multiline_decorator
            ),
            self.correct_lines_with_multiline_decorator,
        )

    def test2(self) -> None:
        """Test no changes to be made."""
        corrected = pslints._class_method_order_enforcer(self.correct_lines)
        self.assertEqual(corrected, self.correct_lines)

    def test3(self) -> None:
        """Test incorrect order."""
        lines = [
            "import math",
            "class Mather:",
            "    def __init__(c1: int, c2: int):",
            "        self._comb = math.comb(c1, c2)",
            "",
            "    def _whisper(self):",
            "        print('it is {self._comb}')",
            "",
            "    def shout(self):",
            "        print(f'IT IS {self._comb}')",
            "",
        ]
        corrected = pslints._class_method_order_enforcer(lines)
        self.assertEqual(self.correct_lines, corrected)

        hinted = pslints._class_method_order_detector("test.py", lines)
        self.assertEqual(
            ["test.py:6: method `_whisper` is located on the wrong line"], hinted
        )

    def test4(self) -> None:
        """Test incorrect order with decorator."""
        lines = [
            "import math",
            "class Mather:",
            "    def __init__(c1: int, c2: int):",
            "        self._comb = math.comb(c1, c2)",
            "",
            "    @staticmethod",
            "    def _whisper(self):",
            "        print('it is {self._comb}')",
            "",
            "    def shout(self):",
            "        print(f'IT IS {self._comb}')",
            "",
        ]
        corrected = pslints._class_method_order_enforcer(lines)
        self.assertEqual(corrected, self.correct_lines_with_decorator)

        hinted = pslints._class_method_order_detector("test.py", lines)
        self.assertEqual(
            ["test.py:7: method `_whisper` is located on the wrong line"], hinted,
        )

    def test5(self) -> None:
        """Test incorrect order with multiline decorator."""
        lines = [
            "import math",
            "class Mather:",
            "    def __init__(c1: int, c2: int):",
            "        self._comb = math.comb(c1, c2)",
            "",
            "    @staticmethod(",
            "       arg=1,",
            "       param=2)",
            "    def _whisper(self):",
            "        print('it is {self._comb}')",
            "",
            "    def shout(self):",
            "        print(f'IT IS {self._comb}')",
            "",
        ]
        corrected = pslints._class_method_order_enforcer(lines)
        self.assertEqual(corrected, self.correct_lines_with_multiline_decorator)

        hinted = pslints._class_method_order_detector("test.py", lines)
        self.assertEqual(
            ["test.py:9: method `_whisper` is located on the wrong line"], hinted,
        )

    def test6(self) -> None:
        """Test `is_class_declaration`"""
        examples = [
            ("class ExampleClass1:", "ExampleClass1"),
            ("class ExampleClass2(ExampleClass1):", "ExampleClass2"),
            ("class ExampleClass2", ""),
            ("cls ExampleClass2:", ""),
        ]
        for example, expected in examples:
            result = pslints._is_class_declaration(example)
            self.assertEqual(expected, result)

    def test7(self) -> None:
        """Test `is_function_declaration`"""
        examples = [
            ("def example_function1(arg1):", "example_function1"),
            ("def example_function2():", "example_function2"),
            ("def example_function:", ""),
            ("def example_function()", ""),
            ("def example_function", ""),
        ]
        for example, expected in examples:
            result = pslints._is_function_declaration(example)
            self.assertEqual(expected, result)
