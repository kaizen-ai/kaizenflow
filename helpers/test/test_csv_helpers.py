import logging
import os

import pandas as pd

import helpers.csv_helpers as hcsh
import helpers.unit_test as huntes

_LOG = logging.getLogger(__name__)


class Test_convert_csv_to_dict(huntes.TestCase):
    def test1(self) -> None:
        dir_name = self.get_input_dir()
        test_csv_path = os.path.join(dir_name, "test.csv")
        actual_result = hcsh.convert_csv_to_dict(test_csv_path, remove_nans=True)
        expected_result = {
            "col1": ["a", "b", "c", "d"],
            "col2": ["a", "b"],
            "col3": ["a", "b", "c"],
        }
        self.assertEqual(actual_result, expected_result)


class Test_from_typed_csv(huntes.TestCase):
    """
    This test is aimed to check the opportunity to load correctly.

    .csv file with dtype param, which exist in .types prefix file. And
    finally it checks that dtypes of loaded dataframe didn't change
    compared with the original one.
    """

    def test1(self) -> None:
        dir_name = self.get_input_dir()
        test_csv_path = os.path.join(dir_name, "test.csv")
        os.path.join(dir_name, "test.csv.types")
        actual_result = (
            hcsh.from_typed_csv(test_csv_path)
            .dtypes.apply(lambda x: x.name)
            .to_dict()
        )
        expected_result = {
            "A": "int64",
            "B": "float64",
            "C": "object",
            "D": "object",
            "E": "int64",
        }
        self.assertEqual(actual_result, expected_result)


class Test_to_typed_csv(huntes.TestCase):
    """
    This test is aimed to check whether the function 'to_typed_csv' create file
    with '.types' prefix or not.
    """

    def test1(self) -> None:
        dir_name = self.get_input_dir()
        test_csv_path = os.path.join(dir_name, "test.csv")
        test_csv_types_path = os.path.join(dir_name, "test.csv.types")
        df = pd.read_csv(test_csv_path)
        hcsh.to_typed_csv(df, test_csv_path)
        self.assertTrue(os.path.exists(test_csv_types_path))
        os.remove(test_csv_types_path)
