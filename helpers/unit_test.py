import inspect
import logging
import os
import pprint
import random
import re
import unittest

import numpy as np

import helpers.dbg as dbg
import helpers.io_ as io_
import helpers.printing as pri
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)

# Global setter / getter for updating test.

# This controls whether the output of a test is updated or not.
_UPDATE_TESTS = False


def set_update_tests(val):
    global _UPDATE_TESTS
    _UPDATE_TESTS = val


def get_update_tests():
    return _UPDATE_TESTS


# Global setter / getter for incremental mode.

# This is useful when a long test wants to reuse some data already generated.
_INCREMENTAL_TESTS = False


def set_incremental_tests(val):
    global _INCREMENTAL_TESTS
    _INCREMENTAL_TESTS = val


def get_incremental_tests():
    return _INCREMENTAL_TESTS


# #############################################################################


def get_random_df(num_cols, seed=None, **kwargs):
    """
    Compute df with random data with `num_cols` columns and index obtained by
    calling `pd.date_range(**kwargs)`.

    :return: df
    """
    import pandas as pd

    if seed:
        np.random.seed(seed)
    dt = pd.date_range(**kwargs)
    df = pd.DataFrame(np.random.rand(len(dt), num_cols), index=dt)
    return df


def get_df_signature(df, num_rows=3):
    import pandas as pd

    dbg.dassert_isinstance(df, pd.DataFrame)
    txt = []
    txt.append("df.shape=%s" % str(df.shape))
    with pd.option_context(
        "display.max_colwidth", int(1e6), "display.max_columns", None
    ):
        txt.append("df.head=\n%s" % df.head(num_rows))
        txt.append("df.tail=\n%s" % df.tail(num_rows))
    txt = "\n".join(txt)
    return txt


# #############################################################################
# Helpers.
# #############################################################################


def _remove_spaces(obj):
    string = str(obj)
    string = string.replace("\\n", "\n").replace("\\t", "\t")
    # Convert multiple empty spaces (but not newlines) into a single one.
    string = re.sub(r"[^\S\n]+", " ", string)
    # Remove insignificant crap.
    lines = []
    for line in string.split("\n"):
        # Remove leading and trailing spaces.
        line = re.sub(r"^\s+", "", line)
        line = re.sub(r"\s+$", "", line)
        # Skip empty lines.
        if line != "":
            lines.append(line)
    string = "\n".join(lines)
    return string


def _assert_equal(actual, expected, full_test_name, test_dir, fuzzy_match=False):
    """
    Implement a better version of self.assertEqual() that reports mismatching
    strings with sdiff and save them to files for further analysis with
    vimdiff.

    :param fuzzy: ignore differences in spaces and end of lines (see
      `_remove_spaces`)
    """

    def _to_string(obj):
        if isinstance(obj, dict):
            ret = pprint.pformat(obj)
        else:
            ret = str(obj)
        ret = ret.rstrip("\n")
        return ret

    # Convert to strings.
    actual = _to_string(actual)
    expected = _to_string(expected)
    # Fuzzy match, if needed.
    if fuzzy_match:
        _LOG.debug("Useing fuzzy match")
        actual_orig = actual
        actual = _remove_spaces(actual)
        expected_orig = expected
        expected = _remove_spaces(expected)
    # Check.
    if expected != actual:
        _LOG.info(
            "%s", "\n" + pri.frame("Test %s failed" % full_test_name, "=", 80)
        )
        if fuzzy_match:
            # Set the following var to True to print the purified version (e.g.,
            # tables too large).
            print_purified_version = False
            # print_purified_version = True
            if print_purified_version:
                expected = expected_orig
                actual = actual_orig
        # Dump the actual and expected strings to files.
        _LOG.debug("Actual:\n%s", actual)
        act_file_name = "%s/tmp.actual.txt" % test_dir
        io_.to_file(act_file_name, actual)
        _LOG.debug("Expected:\n%s", expected)
        exp_file_name = "%s/tmp.expected.txt" % test_dir
        io_.to_file(exp_file_name, expected)
        # Diff to screen.
        _, res = si.system_to_string(
            "echo; sdiff -l -w 150 %s %s" % (exp_file_name, act_file_name),
            abort_on_error=False,
            log_level=logging.DEBUG,
        )
        _LOG.error(res)
        # Report how to diff.
        vimdiff_cmd = "vimdiff %s %s" % (
            os.path.abspath(act_file_name),
            os.path.abspath(exp_file_name),
        )
        # Save a script to diff.
        diff_script = "./tmp_diff.sh"
        io_.to_file(diff_script, vimdiff_cmd)
        cmd = "chmod +x " + diff_script
        si.system(cmd)
        msg = (
            "Diff with:",
            "> " + vimdiff_cmd,
            "or running:",
            "> " + diff_script,
        )
        msg = "\n".join(msg)
        _LOG.error(msg)
        # Print stack trace.
        raise RuntimeError(msg)


# #############################################################################
# TestCase
# #############################################################################


class TestCase(unittest.TestCase):
    """
    Class adding some auxiliary functions to make easy to save output of tests
    as txt.
    """

    def setUp(self):
        random.seed(20000101)
        np.random.seed(20000101)

    def tearDown(self):
        pass

    def create_io_dirs(self):
        dir_name = self.get_input_dir()
        io_.create_dir(dir_name, incremental=True)
        _LOG.info("Creating dir_name=%s", dir_name)
        dir_name = self.get_output_dir()
        io_.create_dir(dir_name, incremental=True)
        _LOG.info("Creating dir_name=%s", dir_name)

    def get_input_dir(self, test_class_name=None, test_method_name=None):
        """
        Return the path of the directory storing input data for this test class.

        :return: dir name
        :rtype: str
        """
        dir_name = (
            self._get_current_path(
                test_class_name=test_class_name, test_method_name=test_method_name
            )
            + "/input"
        )
        return dir_name

    def get_output_dir(self):
        """
        Return the path of the directory storing output data for this test class.

        :return: dir name
        :rtype: str
        """
        dir_name = self._get_current_path() + "/output"
        return dir_name

    def get_scratch_space(self):
        """
        Return the path of the directory storing scratch data for this test class.
        The directory is also created and cleaned up based on whether the
        incremental behavior is enabled or not.

        :return: dir name
        :rtype: str
        """
        dir_name = self._get_current_path()
        io_.create_dir(dir_name, incremental=get_incremental_tests())
        return dir_name

    def assert_equal(self, actual, expected):
        dbg.dassert_in(type(actual), (bytes, str))
        dbg.dassert_in(type(expected), (bytes, str))
        #
        dir_name = self._get_current_path()
        _LOG.debug("dir_name=%s", dir_name)
        io_.create_dir(dir_name, incremental=True)
        dbg.dassert_exists(dir_name)
        test_name = self._get_test_name()
        _assert_equal(actual, expected, test_name, dir_name)

    def check_string(self, actual, fuzzy_match=False):
        """
        Check the actual outcome of a test against the expected outcomes
        contained in the file and/or updates the golden reference file with the
        actual outcome.

        :param: actual
        :type: str or unicode

        Raises if there is an error.
        """
        dbg.dassert_in(type(actual), (bytes, str))
        #
        dir_name = self._get_current_path()
        _LOG.debug("dir_name=%s", dir_name)
        io_.create_dir(dir_name, incremental=True)
        dbg.dassert_exists(dir_name)
        # Get the expected outcome.
        file_name = self.get_output_dir() + "/test.txt"
        _LOG.debug("file_name=%s", file_name)
        if get_update_tests():
            # Update the test result.
            outcome_updated = False
            file_exists = os.path.exists(file_name)
            if file_exists:
                # The golden outcome exists.
                expected = io_.from_file(file_name, split=False)
                if expected != actual:
                    outcome_updated = True
            else:
                # The golden outcome doesn't exist.
                outcome_updated = True
                io_.to_file(file_name, actual)
                # Add to git.
                cmd = "git add %s" % file_name
                rc = si.system(cmd, abort_on_error=False)
                if rc:
                    _LOG.warning(
                        "Can't run '%s': you need to add the file " "manually",
                        cmd,
                    )
            if outcome_updated:
                _LOG.warning("Test outcome updated ... ")
                io_.to_file(file_name, actual)
        else:
            # Just check the test result.
            if os.path.exists(file_name):
                # Golden outcome is available: check the actual outcome against
                # the golden outcome.
                expected = io_.from_file(file_name, split=False)
                test_name = self._get_test_name()
                _assert_equal(
                    actual, expected, test_name, dir_name, fuzzy_match=fuzzy_match
                )
            else:
                # No golden outcome available: save the result in a tmp file.
                tmp_file_name = file_name + ".tmp"
                io_.to_file(tmp_file_name, actual)
                msg = "Can't find golden in %s: saved actual outcome in %s" % (
                    file_name,
                    tmp_file_name,
                )
                raise RuntimeError(msg)

    def _get_test_name(self):
        """
        :return: full test name as class.method.
        :rtype: str
        """
        return "/%s.%s" % (self.__class__.__name__, self._testMethodName)

    def _get_current_path(self, test_class_name=None, test_method_name=None):
        dir_name = os.path.dirname(inspect.getfile(self.__class__))
        if test_class_name is None:
            test_class_name = self.__class__.__name__
        if test_method_name is None:
            test_method_name = self._testMethodName
        dir_name = dir_name + "/%s.%s" % (test_class_name, test_method_name)
        return dir_name
