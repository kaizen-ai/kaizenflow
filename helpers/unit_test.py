import inspect
import logging
import os
import pprint
import random
import re
import types
import unittest

import numpy as np

import helpers.dbg as dbg
import helpers.helper_io as io_
import helpers.printing as print_
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
# Helpers.
# #############################################################################


def _assert_equal(actual, expected, full_test_name, test_dir):
    """
    Implement a better version of self.assertEqual() that reports mismatching
    strings with sdiff and save them to files for further analysis with vimdiff.
    """

    def _to_string(obj):
        if isinstance(obj, dict):
            ret = pprint.pformat(obj)
        else:
            ret = str(obj)
        ret = ret.rstrip("\n")
        return ret

    actual = _to_string(actual)
    expected = _to_string(expected)
    if expected != actual:
        _LOG.info("%s", "\n" +
                  print_.frame("Test %s failed" % full_test_name, "=", 80))
        # Dump the expected and actual strings to files.
        _LOG.debug("Expected:\n%s", expected)
        exp_file_name = "%s/tmp.expected.txt" % test_dir
        io_.to_file(exp_file_name, expected)
        _LOG.debug("Actual:\n%s", actual)
        act_file_name = "%s/tmp.actual.txt" % test_dir
        io_.to_file(act_file_name, actual)
        # Diff to screen.
        _, res = si.system_to_string(
            "echo; sdiff -l -w 150 %s %s" % (exp_file_name, act_file_name),
            abort_on_error=False,
            log_level=logging.DEBUG)
        _LOG.error(res)
        # Report how to diff.
        vimdiff_cmd = "vimdiff %s %s" % (os.path.abspath(exp_file_name),
                                         os.path.abspath(act_file_name))
        # Save a script to diff.
        diff_script = "./tmp_diff.sh"
        io_.to_file(diff_script, vimdiff_cmd)
        cmd = "chmod +x " + diff_script
        si.system(cmd)
        # Print stack trace.
        msg = "\nDiff with:\n" + vimdiff_cmd + "\nor running:\n" + diff_script
        _LOG.error(msg)
        # Print stack trace.
        raise RuntimeError(msg)


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


def _fuzzy_assert_equal(actual, expected, full_test_name):
    """
    Implement a better version of self.assertEqual() that ignores differences in
    spaces and end of lines, by calling _remove_spaces().
    """
    purified_actual = _remove_spaces(actual)
    purified_expected = _remove_spaces(expected)
    if purified_expected != purified_actual:
        # Set the following var to True to print the purified version (e.g.,
        # tables too large).
        print_purified_version = False
        # print_purified_version = True
        if print_purified_version:
            expected = purified_expected
            actual = purified_actual
        else:
            expected = expected
            actual = actual
        #
        _assert_equal(actual, expected, full_test_name)


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
        dir_name = self._get_current_path(
            test_class_name=test_class_name,
            test_method_name=test_method_name) + "/input"
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

    def check_string(self, actual):
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
            if outcome_updated:
                _LOG.warning("Test outcome updated ... ")
                io_.to_file(file_name, actual)
        else:
            # Just check the test result.
            if os.path.exists(file_name):
                # Golden outcome is available: check the actual outcome against the
                # golden outcome.
                expected = io_.from_file(file_name, split=False)
                test_name = self._get_test_name()
                _assert_equal(actual, expected, test_name, dir_name)
            else:
                # No golden outcome available: save the result in a tmp file.
                tmp_file_name = file_name + ".tmp"
                io_.to_file(tmp_file_name, actual)
                msg = "Can't find golden in %s: saved actual outcome in %s" % (
                    file_name, tmp_file_name)
                raise RuntimeError(msg)

    def _get_test_name(self):
        """
        :return: full test name as class.method.
        :rtype: str
        """
        return "/%s.%s" % (self.__class__.__name__, self._testMethodName)

    def _get_current_path(self, test_class_name=None, test_method_name=None):
        """

        :return:
        """
        dir_name = os.path.dirname(inspect.getfile(self.__class__))
        if test_class_name is None:
            test_class_name = self.__class__.__name__
        if test_method_name is None:
            test_method_name = self._testMethodName
        dir_name = dir_name + "/%s.%s" % (test_class_name, test_method_name)
        return dir_name
