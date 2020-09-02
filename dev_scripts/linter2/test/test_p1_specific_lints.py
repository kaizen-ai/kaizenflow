import pytest

import dev_scripts.linter2.p1_specific_lints as pslints
import helpers.unit_test as hut


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


@pytest.mark.skip(reason="Need to install mock")
class Test_check_shebang(hut.TestCase):
    def _helper_check_shebang(
        self, file_name: str, txt: str, is_executable: bool, exp: str,
    ) -> None:
        import mock

        txt_array = txt.split("\n")

        with mock.patch("os.access", return_value=is_executable):
            msg = pslints._check_shebang(file_name, txt_array)
        self.assert_equal(msg, exp)

    def test1(self) -> None:
        """Executable with wrong shebang: error."""
        file_name = "exec.py"
        txt = """#!/bin/bash
hello
world
"""
        is_executable = True
        exp = "exec.py:1: any executable needs to start with a shebang '#!/usr/bin/env python'"
        self._helper_check_shebang(file_name, txt, is_executable, exp)

    def test2(self) -> None:
        """Executable with the correct shebang: correct."""
        file_name = "exec.py"
        txt = """#!/usr/bin/env python
hello
world
"""
        is_executable = True
        exp = ""
        self._helper_check_shebang(file_name, txt, is_executable, exp)

    def test3(self) -> None:
        """Non executable with a shebang: error."""
        file_name = "exec.py"
        txt = """#!/usr/bin/env python
hello
world
"""
        is_executable = False
        exp = "exec.py:1: a non-executable can't start with a shebang."
        self._helper_check_shebang(file_name, txt, is_executable, exp)

    def test4(self) -> None:
        """Library without a shebang: correct."""
        file_name = "lib.py"
        txt = '''"""
Import as:

import _setenv_lib as selib
'''
        is_executable = False
        exp = ""
        self._helper_check_shebang(file_name, txt, is_executable, exp)

