import toml
from io import StringIO
from typing import Any, Dict

import pandas as pd
import pprint

import helpers.unit_test as hut
import dev_scripts.toml_merge as toml_merge


def _to_toml(txt: str) -> None:
    """
    Remove all empty lines and leading / trailing spaces.
    """
    txt = "\n".join([line.rstrip().lstrip() for line in txt.split("\n") if txt])
    return toml.load(StringIO(txt))


class TestMergeToml(hut.TestCase):

    @staticmethod
    def _get_pyproj1() -> Dict[str, Any]:
        pyproj = """
        [tool.poetry]
        name = "lem"
        version = "0.1.0"
        description = ""
        authors = [""]

        [tool.poetry.dependencies]
        awscli = "*"
        boto3 = "*"

        [tool.poetry.dev-dependencies]

        [build-system]
        requires = ["poetry>=0.12"]
        build-backend = "poetry.masonry.api"
        """
        return pyproj

    def test1(self) -> None:
        """
        Test that merging two equal toml files return the same one.
        """
        # Define input variables.
        pyproj1 = self._get_pyproj1()
        pyproj2 = self._get_pyproj1()
        pyprojs = [_to_toml(pyproj1), _to_toml(pyproj2)]
        # Call function to test.
        act = toml_merge._merge_toml(pyprojs=pyprojs)
        # Define expected output.
        exp = pyproj1
        exp = _to_toml(exp)
        # Compare actual and expected output.
        self.assert_equal(pprint.pformat(act), pprint.pformat(exp))

    def test2(self) -> None:
        """
        Test merging two toml files.
        """
        # Define input variables.
        pyproj1 = self._get_pyproj1()
        pyproj2 = """
        [tool.poetry]
        name = "amp"
        version = "0.1.0"
        description = ""
        authors = [""]

        [tool.poetry.dependencies]
        awscli = "*"
        boto3 = "*"
        bs4 = "*"
        flaky = "*"

        [tool.poetry.dev-dependencies]

        [build-system]
        requires = ["poetry>=0.12"]
        build-backend = "poetry.masonry.api"
        """
        pyprojs = [_to_toml(pyproj1), _to_toml(pyproj2)]
        # Call function to test.
        act = toml_merge._merge_toml(pyprojs=pyprojs)
        # Define expected output.
        exp = """
        [tool.poetry]
        name = "lem"
        version = "0.1.0"
        description = ""
        authors = [""]

        [tool.poetry.dependencies]
        awscli = "*"
        boto3 = "*"
        bs4 = "*"
        flaky = "*"

        [tool.poetry.dev-dependencies]

        [build-system]
        requires = ["poetry>=0.12"]
        build-backend = "poetry.masonry.api"
        """
        exp = _to_toml(exp)
        # Compare actual and expected output.
        self.assert_equal(pprint.pformat(act), pprint.pformat(exp))

    def test3(self) -> None:
        """
        Test that an incompatible constraint (`awscli = "*"` and `awscli = "1.0"`)
        asserts.
        """
        # Define input variables.
        pyproj1 = self._get_pyproj1()
        pyproj2 = """
        [tool.poetry]
        name = "amp"
        version = "0.1.0"
        description = ""
        authors = [""]

        [tool.poetry.dependencies]
        awscli = "1.0"
        boto3 = "*"
        bs4 = "*"
        flaky = "*"
        """
        pyprojs = [_to_toml(pyproj1), _to_toml(pyproj2)]
        # Call function to test.
        with self.assertRaises(ValueError):
            _ = toml_merge._merge_toml(pyprojs=pyprojs)
