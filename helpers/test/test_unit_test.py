import datetime
import unittest.mock as umock
import uuid

import pandas as pd
import pytest

import helpers.git as git
import helpers.unit_test as hut

import tempfile


class TestTestCase(hut.TestCase):

    def test_get_input_dir1(self):
        """
        Test hut.get_input_dir().
        """
        act = self.get_input_dir()
        act = hut.purify_txt_from_client(act)
        exp = "$GIT_ROOT/helpers/test/TestTestCase.test_get_dir1/input"
        self.assertEqual(act, exp)

    def test_get_input_dir2(self):
        test_class_name = "test_class"
        test_method_name = "test_method"
        act = self.get_input_dir(test_class_name, test_method_name)
        act = hut.purify_txt_from_client(act)
        #
        exp = "$GIT_ROOT/helpers/test/test_class.test_method/input"
        self.assertEqual(act, exp)

    def test_get_output_dir1(self):
        """
        Test hut.get_output_dir().
        """
        act = self.get_output_dir()
        act = hut.purify_txt_from_client(act)
        exp = "$GIT_ROOT/helpers/test/TestTestCase.test_get_output_dir1/output"
        self.assertEqual(act, exp)

    def test_get_scratch_space1(self):
        """
        Test hut.get_scratch_space().
        """
        act = self.get_scratch_space()
        act = hut.purify_txt_from_client(act)
        exp = "$GIT_ROOT/helpers/test/TestTestCase.test_get_scratch_space1/tmp.scratch"
        self.assertEqual(act, exp)

    def test_get_scratch_space2(self):
        test_class_name = "test_class"
        test_method_name = "test_method"
        act = self.get_scratch_space(test_class_name, test_method_name)
        act = hut.purify_txt_from_client(act)
        exp = "$GIT_ROOT/helpers/test/test_class.test_method/tmp.scratch"
        self.assertEqual(act, exp)

    def test_assert_equal1(self):
        actual = "hello world"
        expected = actual
        self.assert_equal(actual, expected)

    def test_assert_not_equal1(self):
        actual = "hello world"
        expected = "hello world "
        tmp_dir = tempfile.gettempdir()
        with self.assertRaises(RuntimeError) as cm:
            self.assert_equal(actual, expected, dst_dir=tmp_dir)
        self.check_string(str(cm))

    def test_assert_not_equal2(self):
        actual = "hello world"
        expected = "hello world "
        tmp_dir = tempfile.gettempdir()
        self.assert_equal(actual, expected, assert_on_error=False, dst_dir=tmp_dir)
        # Compute the signature from the dir.
        act = hut.get_dir_signature(tmp_dir)
        exp = ""
        self.assertEqual(act, exp)

    def test_assert_equal_fuzzy_match1(self):
        actual = "hello world"
        expected = "hello world "
        is_equal = self.assert_equal(actual, expected, fuzzy_match=True)
        self.assertTrue(is_equal)

    def test_assert_equal5(self):
        actual = "hello world"
        expected = "hello world2"
        with self.assertRaises(RuntimeError) as cm:
            self.assert_equal(actual, expected, fuzzy_match=True)

    def test_check_string1(self):
        act = "hello world"
        self.check_string(act)

    def test_check_string2(self):
        base_dir_name = tempfile.gettempdir()
        self.set_base_dir_name(base_dir_name)
        #input_dir



class Test_unit_test1(hut.TestCase):
    @pytest.mark.not_docker
    @pytest.mark.amp
    def test_purify_txt_from_client1(self) -> None:
        super_module_path = git.get_client_root(super_module=True)
        # TODO(gp): We should remove the current path.
        txt = r"""
************* Module input [pylint]
$SUPER_MODULE/dev_scripts/test/Test_linter_py1.test_linter1/tmp.scratch/input.py: Your code has been rated at -10.00/10 (previous run: -10.00/10, +0.00) [pylint]
$SUPER_MODULE/dev_scripts/test/Test_linter_py1.test_linter1/tmp.scratch/input.py:3:20: W605 invalid escape sequence '\s' [flake8]
$SUPER_MODULE/dev_scripts/test/Test_linter_py1.test_linter1/tmp.scratch/input.py:3:9: F821 undefined name 're' [flake8]
cmd line='$SUPER_MODULE/dev_scripts/linter.py -f $SUPER_MODULE/amp/dev_scripts/test/Test_linter_py1.test_linter1/tmp.scratch/input.py --linter_log $SUPER_MODULE/dev_scripts/test/Test_linter_py1.test_linter1/tmp.scratch/linter.log'
dev_scripts/test/Test_linter_py1.test_linter1/tmp.scratch/input.py:3: [E0602(undefined-variable), ] Undefined variable 're' [pylint]
dev_scripts/test/Test_linter_py1.test_linter1/tmp.scratch/input.py:3: [W1401(anomalous-backslash-in-string), ] Anomalous backslash in string: '\s'. String constant might be missing an r prefix. [pylint]
dev_scripts/test/Test_linter_py1.test_linter1/tmp.scratch/input.py:3: error: Name 're' is not defined [mypy]
"""
        txt = txt.replace("$SUPER_MODULE", super_module_path)
        exp = r"""
************* Module input [pylint]
$GIT_ROOT/dev_scripts/test/Test_linter_py1.test_linter1/tmp.scratch/input.py: Your code has been rated at -10.00/10 (previous run: -10.00/10, +0.00) [pylint]
$GIT_ROOT/dev_scripts/test/Test_linter_py1.test_linter1/tmp.scratch/input.py:3:20: W605 invalid escape sequence '\s' [flake8]
$GIT_ROOT/dev_scripts/test/Test_linter_py1.test_linter1/tmp.scratch/input.py:3:9: F821 undefined name 're' [flake8]
cmd line='$GIT_ROOT/dev_scripts/linter.py -f $GIT_ROOT/dev_scripts/test/Test_linter_py1.test_linter1/tmp.scratch/input.py --linter_log $GIT_ROOT/dev_scripts/test/Test_linter_py1.test_linter1/tmp.scratch/linter.log'
dev_scripts/test/Test_linter_py1.test_linter1/tmp.scratch/input.py:3: [E0602(undefined-variable), ] Undefined variable 're' [pylint]
dev_scripts/test/Test_linter_py1.test_linter1/tmp.scratch/input.py:3: [W1401(anomalous-backslash-in-string), ] Anomalous backslash in string: '\s'. String constant might be missing an r prefix. [pylint]
dev_scripts/test/Test_linter_py1.test_linter1/tmp.scratch/input.py:3: error: Name 're' is not defined [mypy]
"""
        act = hut.purify_txt_from_client(txt)
        self.assert_equal(act, exp)

    def test_purify_txt_from_client2(self) -> None:
        """
        Test case when client root path is equal to `/`
        """
        git = umock.Mock()
        git.get_client_root.return_value = "/"
        txt = "/tmp/subdir1"
        exp = txt
        act = hut.purify_txt_from_client(txt)
        self.assertEqual(act, exp)


class TestDataframeToJson(hut.TestCase):
    def test_dataframe_to_json(self) -> None:
        """
        Verify correctness of dataframe to JSON transformation.
        """
        # Initialize a dataframe.
        test_dataframe = pd.DataFrame(
            {
                "col_1": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0],
                "col_2": [1, 2, 3, 4, 5, 6, 7],
            }
        )
        # Convert dataframe to JSON.
        output_str = hut.convert_df_to_json_string(
            test_dataframe, n_head=3, n_tail=3
        )
        self.check_string(output_str)

    def test_dataframe_to_json_uuid(self) -> None:
        """
        Verify correctness of UUID-containing dataframe transformation.
        """
        # Initialize a dataframe.
        test_dataframe = pd.DataFrame(
            {
                "col_1": [
                    uuid.UUID("421470c7-7797-4a94-b584-eb83ff2de88a"),
                    uuid.UUID("22cde381-1782-43dc-8c7a-8712cbdf5ee1"),
                ],
                "col_2": [1, 2],
            }
        )
        # Convert dataframe to JSON.
        output_str = hut.convert_df_to_json_string(
            test_dataframe, n_head=None, n_tail=None
        )
        self.check_string(output_str)

    def test_dataframe_to_json_timestamp(self) -> None:
        """
        Verify correctness of transformation of a dataframe with Timestamps.
        """
        # Initialize a dataframe.
        test_dataframe = pd.DataFrame(
            {
                "col_1": [pd.Timestamp("2020-01-01"), pd.Timestamp("2020-05-12")],
                "col_2": [1.0, 2.0],
            }
        )
        # Convert dataframe to JSON.
        output_str = hut.convert_df_to_json_string(
            test_dataframe, n_head=None, n_tail=None
        )
        self.check_string(output_str)

    def test_dataframe_to_json_datetime(self) -> None:
        """
        Verify correctness of transformation of a dataframe with datetime.
        """
        # Initialize a dataframe.
        test_dataframe = pd.DataFrame(
            {
                "col_1": [
                    datetime.datetime(2020, 1, 1),
                    datetime.datetime(2020, 5, 12),
                ],
                "col_2": [1.0, 2.0],
            }
        )
        # Convert dataframe to JSON.
        output_str = hut.convert_df_to_json_string(
            test_dataframe, n_head=None, n_tail=None
        )
        self.check_string(output_str)
