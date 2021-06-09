import logging
import os

import numpy as np
import pandas as pd

import helpers.csv_helpers as hchelp
import helpers.env as henv
import helpers.io_ as hio
import helpers.printing as hprint
import helpers.s3 as hs3
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


# #############################################################################
# helpers.py
# #############################################################################


class Test_convert_csv_to_dict(hut.TestCase):
    def test1(self) -> None:
        dir_name = self.get_input_dir()
        test_csv_path = os.path.join(dir_name, "test.csv")
        actual_result = hchelp.convert_csv_to_dict(
            test_csv_path, remove_nans=True
        )
        expected_result = {
            "col1": ["a", "b", "c", "d"],
            "col2": ["a", "b"],
            "col3": ["a", "b", "c"],
        }
        self.assertEqual(actual_result, expected_result)


class Test_from_typed_csv(hut.TestCase):
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
            hchelp.from_typed_csv(test_csv_path)
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


class Test_to_typed_csv(hut.TestCase):
    """
    This test is aimed to check whether the function 'to_typed_csv' create file
    with '.types' prefix or not.
    """

    def test1(self) -> None:
        dir_name = self.get_input_dir()
        test_csv_path = os.path.join(dir_name, "test.csv")
        test_csv_types_path = os.path.join(dir_name, "test.csv.types")
        df = pd.read_csv(test_csv_path)
        hchelp.to_typed_csv(df, test_csv_path)
        self.assertTrue(os.path.exists(test_csv_types_path))
        os.remove(test_csv_types_path)


# #############################################################################
# henv.py
# #############################################################################


class Test_env1(hut.TestCase):
    def test_get_system_signature1(self) -> None:
        txt = henv.get_system_signature()
        _LOG.debug(txt)


# #############################################################################
# hio.py
# #############################################################################


class Test_load_df_from_json(hut.TestCase):
    def test1(self) -> None:
        test_json_path = os.path.join(self.get_input_dir(), "test.json")
        actual_result = hio.load_df_from_json(test_json_path)
        expected_result = pd.DataFrame(
            {
                "col1": ["a", "b", "c", "d"],
                "col2": ["a", "b", np.nan, np.nan],
                "col3": ["a", "b", "c", np.nan],
            }
        )
        actual_result = hprint.dataframe_to_str(actual_result)
        expected_result = hprint.dataframe_to_str(expected_result)
        self.assertEqual(actual_result, expected_result)


# #############################################################################
# numba.py
# #############################################################################


class Test_numba_1(hut.TestCase):
    def test1(self) -> None:
        # TODO(gp): Implement this.
        pass


# #############################################################################
# s3.py
# #############################################################################


class Test_s3_1(hut.TestCase):
    def test_get_path1(self) -> None:
        file_path = "s3://alphamatic-data/data/kibot/All_Futures_Continuous_Contracts_daily"
        bucket_name, file_path = hs3.parse_path(file_path)
        self.assertEqual(bucket_name, "alphamatic-data")
        self.assertEqual(
            file_path, "data/kibot/All_Futures_Continuous_Contracts_daily"
        )

    def test_ls1(self) -> None:
        file_path = os.path.join(hs3.get_path(), "README.md")
        _LOG.debug("file_path=%s", file_path)
        # > aws s3 ls s3://alphamatic-data
        #                   PRE data/
        # 2021-04-06 1:17:44 48 README.md
        file_names = hs3.ls(file_path)
        self.assertGreater(len(file_names), 0)
