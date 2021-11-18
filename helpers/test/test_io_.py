import logging
import os

import numpy as np
import pandas as pd

import helpers.git as hgit
import helpers.io_ as hio
import helpers.printing as hprint
import helpers.unit_test as hunitest

_LOG = logging.getLogger(__name__)


class Test_find_all_files1(hunitest.TestCase):
    def test1(self) -> None:
        dir_name = hgit.get_client_root(super_module=False)
        #
        # Check that there are files.
        all_files = hio.find_all_files(dir_name)
        self.assertGreater(len(all_files), 0)
        # Check that there are more files than Python files.
        exclude_paired_jupytext = False
        py_files = hio.keep_python_files(all_files, exclude_paired_jupytext)
        self.assertGreater(len(py_files), 0)
        self.assertGreater(len(all_files), len(py_files))
        # Check that there are more Python files than not paired Python files.
        exclude_paired_jupytext = True
        not_paired_py_files = hio.keep_python_files(
            all_files, exclude_paired_jupytext
        )
        self.assertGreater(len(not_paired_py_files), 0)
        self.assertGreater(len(py_files), len(not_paired_py_files))


class Test_change_filename_extension1(hunitest.TestCase):
    def test1(self) -> None:
        file_name = "./core/dataflow_model/notebooks/Master_experiment_runner.py"
        actual = hio.change_filename_extension(file_name, "py", "ipynb")
        expected = (
            "./core/dataflow_model/notebooks/Master_experiment_runner.ipynb"
        )
        self.assert_equal(actual, expected)


class Test_load_df_from_json(hunitest.TestCase):
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
