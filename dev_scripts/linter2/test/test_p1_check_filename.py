import dev_scripts.linter2.p1_check_filename as pcf
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
        msg = pcf._check_notebook_dir(file_name)
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
        msg = pcf._check_test_file_dir(file_name)
        self.assert_equal(msg, exp)


class Test_check_notebook_filename(hut.TestCase):
    def test1(self) -> None:
        r"""Check python files are not checked

        - Given python file
        - When function runs
        - Then no warning message is returned"""
        file_name = "linter/module.py"
        actual = pcf._check_notebook_filename(file_name)
        self.assertEqual("", actual)

    def test2(self) -> None:
        r"""Check filename rules

        - Given notebook filename starts with `Master_`
        - When function runs
        - Then no warning message is returned"""
        file_name = "linter/Master_notebook.ipynb"
        actual = pcf._check_notebook_filename(file_name)
        self.assertEqual("", actual)

    def test3(self) -> None:
        r"""Check filename rules

        - Given notebook filename matchs `\S+Task\d+_...`
        - When function runs
        - Then no warning message is returned"""
        file_name = "linter/PartTask400_test.ipynb"
        actual = pcf._check_notebook_filename(file_name)
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
        actual = pcf._check_notebook_filename(file_name)
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
        actual = pcf._check_notebook_filename(file_name)
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
        actual = pcf._check_notebook_filename(file_name)
        self.assertEqual(exp, actual)
