from typing import List

import dev_scripts.linter2.p1_class_method_order as pcmo
import helpers.unit_test as hut


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
        correct_lines_nodes = pcmo._lines_to_nodes(self.correct_lines)
        self.assertEqual(
            pcmo._nodes_to_lines_in_correct_order(correct_lines_nodes),
            self.correct_lines,
        )
        correct_lines_decorator = pcmo._lines_to_nodes(
            self.correct_lines_with_decorator
        )
        self.assertEqual(
            pcmo._nodes_to_lines_in_correct_order(correct_lines_decorator),
            self.correct_lines_with_decorator,
        )
        correct_lines_multiline_decorator = pcmo._lines_to_nodes(
            self.correct_lines_with_multiline_decorator
        )
        self.assertEqual(
            pcmo._nodes_to_lines_in_correct_order(
                correct_lines_multiline_decorator
            ),
            self.correct_lines_with_multiline_decorator,
        )

    def test2(self) -> None:
        """Test no changes to be made."""
        corrected = pcmo._class_method_order_enforcer(self.correct_lines)
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
        corrected = pcmo._class_method_order_enforcer(lines)
        self.assertEqual(self.correct_lines, corrected)

        hinted = pcmo._class_method_order_detector("test.py", lines)
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
        corrected = pcmo._class_method_order_enforcer(lines)
        self.assertEqual(corrected, self.correct_lines_with_decorator)

        hinted = pcmo._class_method_order_detector("test.py", lines)
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
        corrected = pcmo._class_method_order_enforcer(lines)
        self.assertEqual(corrected, self.correct_lines_with_multiline_decorator)

        hinted = pcmo._class_method_order_detector("test.py", lines)
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
            result = pcmo._is_class_declaration(example)
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
            result = pcmo._is_function_declaration(example)
            self.assertEqual(expected, result)
