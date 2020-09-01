import pytest

import dev_scripts.linter2.p1_check_shebang as pcs
import helpers.unit_test as hut


@pytest.mark.skip(reason="Need to install mock")
class Test_check_shebang(hut.TestCase):
    def _helper_check_shebang(
        self, file_name: str, txt: str, is_executable: bool, exp: str,
    ) -> None:
        import mock

        txt_array = txt.split("\n")

        with mock.patch("os.access", return_value=is_executable):
            msg = pcs._check_shebang(file_name, txt_array)
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
