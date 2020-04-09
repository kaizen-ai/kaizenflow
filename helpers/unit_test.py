"""
Import as:

import helpers.unit_test as ut

# TODO(gp): use hut instead of ut.
"""

import inspect
import logging
import os
import pprint
import random
import re
import unittest
from typing import Any, List, NoReturn, Optional, Iterable, Mapping, Union

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import helpers.dbg as dbg
import helpers.git as git
import helpers.io_ as io_
import helpers.printing as prnt
import helpers.system_interaction as si
import collections

_LOG = logging.getLogger(__name__)

# #############################################################################

# Global setter / getter for updating test.

# This controls whether the output of a test is updated or not.
_UPDATE_TESTS = False


def set_update_tests(val: bool) -> None:
    global _UPDATE_TESTS
    _UPDATE_TESTS = val


def get_update_tests() -> bool:
    return _UPDATE_TESTS


# #############################################################################

# Global setter / getter for incremental mode.

# This is useful when a long test wants to reuse some data already generated.
_INCREMENTAL_TESTS = False


def set_incremental_tests(val: bool) -> None:
    global _INCREMENTAL_TESTS
    _INCREMENTAL_TESTS = val


def get_incremental_tests() -> bool:
    return _INCREMENTAL_TESTS


# #############################################################################

_CONFTEST_IN_PYTEST = False


def in_unit_test_mode() -> bool:
    """
    Return True if we are inside a pytest run.
    This is set by conftest.py.
    """
    return _CONFTEST_IN_PYTEST


# #############################################################################


def convert_df_to_string(
    df: Union[pd.DataFrame, pd.Series],
    n_rows: Optional[int] = None,
    title: Optional[str] = None
) -> str:
    """
    Convert DataFrame or Series to string for verifying test results.

    :param df: DataFrame to be verified
    :param n_rows: number of rows in expected output
    :param title: title for test output
    :return: string representation of input
    """
    n_rows = n_rows or len(df)
    output = []
    # Add title in the beginning if provided.
    if title is not None:
        output.append(prnt.frame(title))
    # Provide context for full representation of data.
    with pd.option_context(
                "display.max_colwidth", int(1e6), "display.max_columns", None, "display.max_rows", None
        ):
        # Add top N rows.
        output.append(df.head(n_rows).to_string(index=False))
        output_str = "\n".join(output)
    return output_str


def convert_info_to_string(info: Mapping):
    """
    Convert info to string for verifying test results.

    Info often contains pd.Series, so pandas context is provided
    to print all rows and all contents.

    :param info: info to convert to string
    :return: string representation of info
    """
    output = []
    # Provide context for full representation of pd.Series in info.
    with pd.option_context(
                "display.max_colwidth", int(1e6), "display.max_columns", None, "display.max_rows", None
        ):
        output.append(prnt.frame("info"))
        output.append(pprint.pformat(info))
        output_str = "\n".join(output)
    return output_str


def get_ordered_value_counts(column: pd.Series) -> pd.Series:
    """
    Get column value counts and sort.

    Value counts are sorted alphabetically by index, and then counts in
    descending order. The order of indices with the same count is
    alphabetical, which makes the string representation of the same series
    predictable.

    The output of `value_counts` without sort arranges indices with same
    counts randomly, which makes tests dependent on string comparison
    impossible.

    :param column: column for value counts
    :return: counts ordered by index and values
    """
    value_counts = column.value_counts()
    value_counts = value_counts.sort_index()
    value_counts = value_counts.sort_values(ascending=False)
    return value_counts


def get_value_counts_for_columns(
        df: pd.DataFrame, columns: Optional[Iterable] = None
) -> Mapping[str, pd.Series]:
    """
    Get value counts for multiple columns.

    The function creates a dict of value counts for each passed column. The
    values in each resulting series are sorted first by value, then alphabetically
    by index to keep the order predictable.

    Counts are included in info for filtering and mapping functions to keep
    track of changes in values.

    :param df: dataframe with value counts going to info
    :param columns: names of columns for counting values
    :return: value counts for provided columns
    """
    columns = columns or df.columns.to_list()
    dbg.dassert_is_subset(
        columns,
        df.columns.to_list(),
        msg="The requested columns could not be found in the dataframe",
    )
    value_counts_by_column = collections.OrderedDict()
    for col in columns:
        value_counts_by_column[col] = get_ordered_value_counts(df[col])
    return value_counts_by_column


def to_string(var: str) -> str:
    return """f"%s={%s}""" % (var, var)


def get_random_df(
    num_cols: int, seed: Optional[int] = None, **kwargs: Any
) -> pd.DataFrame:
    """
    Compute df with random data with `num_cols` columns and index obtained by
    calling `pd.date_range(**kwargs)`.

    :return: df
    """
    if seed:
        np.random.seed(seed)
    dt = pd.date_range(**kwargs)
    df = pd.DataFrame(np.random.rand(len(dt), num_cols), index=dt)
    return df


def get_df_signature(df: pd.DataFrame, num_rows: int = 3) -> str:
    dbg.dassert_isinstance(df, pd.DataFrame)
    txt: List[str] = []
    txt.append("df.shape=%s" % str(df.shape))
    with pd.option_context(
        "display.max_colwidth", int(1e6), "display.max_columns", None
    ):
        txt.append("df.head=\n%s" % df.head(num_rows))
        txt.append("df.tail=\n%s" % df.tail(num_rows))
    txt = "\n".join(txt)
    return txt


# TODO(gp): Maybe it's more general than this file.
def filter_text(regex: str, txt: str) -> str:
    """
    Remove lines in `txt` that match the regex `regex`.
    """
    _LOG.debug("Filtering with '%s'", regex)
    if regex is None:
        return txt
    txt_out = []
    txt_as_arr = txt.split("\n")
    for line in txt_as_arr:
        if re.search(regex, line):
            _LOG.debug("Skipping line='%s'", line)
            continue
        txt_out.append(line)
    # We can only remove lines.
    dbg.dassert_lte(
        len(txt_out),
        len(txt_as_arr),
        "txt_out=\n'''%s'''\ntxt=\n'''%s'''",
        "\n".join(txt_out),
        "\n".join(txt_as_arr),
    )
    txt = "\n".join(txt_out)
    return txt


def remove_amp_references(txt: str) -> str:
    """
    Remove references to amp.
    """
    txt = re.sub("^amp/", "", txt, flags=re.MULTILINE)
    txt = re.sub("/amp/", "/", txt, flags=re.MULTILINE)
    txt = re.sub("/amp:", ":", txt, flags=re.MULTILINE)
    return txt


def purify_txt_from_client(txt: str) -> str:
    """
    Remove from a string all the information specific of a git client.
    """
    # We remove references to the Git modules starting from the innermost one.
    for super_module in [False, True]:
        # Replace the git path with `$GIT_ROOT`.
        super_module_path = git.get_client_root(super_module=super_module)
        txt = txt.replace(super_module_path, "$GIT_ROOT")
    # Replace the current path with `$PWD`
    pwd = os.getcwd()
    txt = txt.replace(pwd, "$PWD")
    # Replace the user name with `$USER_NAME`.
    user_name = si.get_user_name()
    txt = txt.replace(user_name, "$USER_NAME")
    # Remove amp reference, if any.
    txt = remove_amp_references(txt)
    # TODO(gp): Remove conda_sh_path.
    return txt


def diff_files(
    file_name1: str, file_name2: str, tag: Optional[str] = None
) -> NoReturn:
    # Diff to screen.
    _, res = si.system_to_string(
        "echo; sdiff -l -w 150 %s %s" % (file_name1, file_name2),
        abort_on_error=False,
        log_level=logging.DEBUG,
    )
    if tag is not None:
        _LOG.error("%s", "\n" + prnt.frame(tag))
    _LOG.error(res)
    # Report how to diff.
    vimdiff_cmd = "vimdiff %s %s" % (
        os.path.abspath(file_name1),
        os.path.abspath(file_name2),
    )
    # Save a script to diff.
    diff_script = "./tmp_diff.sh"
    io_.to_file(diff_script, vimdiff_cmd)
    cmd = "chmod +x " + diff_script
    si.system(cmd)
    msg = []
    msg.append("Diff with:")
    msg.append("> " + vimdiff_cmd)
    msg.append("or running:")
    msg.append("> " + diff_script)
    msg_as_str = "\n".join(msg)
    _LOG.error(msg_as_str)
    raise RuntimeError(msg_as_str)


# #############################################################################


# TODO(gp): Make these functions static of TestCase.
def _remove_spaces(obj: Any) -> str:
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


def _assert_equal(
    actual: str,
    expected: str,
    full_test_name: str,
    test_dir: str,
    fuzzy_match: bool = False,
) -> None:
    """
    Implement a better version of self.assertEqual() that reports mismatching
    strings with sdiff and save them to files for further analysis with
    vimdiff.

    :param fuzzy: ignore differences in spaces and end of lines (see
      `_remove_spaces`)
    """

    def _to_string(obj: str) -> str:
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
            "%s", "\n" + prnt.frame("Test %s failed" % full_test_name, "=", 80)
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
        #
        tag = "ACTUAL vs EXPECTED"
        diff_files(act_file_name, exp_file_name, tag)


class TestCase(unittest.TestCase):
    """
    Class adding some auxiliary functions to make easy to save output of tests
    as txt.
    """

    def setUp(self) -> None:
        random.seed(20000101)
        np.random.seed(20000101)
        # Disable matplotlib plotting by overwriting the `show` function.
        plt.show = lambda: 0
        # Name of the dir with artifacts for this test.
        self._scratch_dir: Optional[str] = None
        # Print banner to signal starting of a new test.
        func_name = "%s.%s" % (self.__class__.__name__, self._testMethodName)
        _LOG.debug("\n%s", prnt.frame(func_name))

    def tearDown(self) -> None:
        # Force matplotlib to close plots to decouple tests.
        plt.close()
        plt.clf()
        # Delete the scratch dir, if needed.
        # TODO(gp): We would like to keep this if the test failed.
        #  I can't find an easy way to detect this situation.
        #  For now just re-run with --incremental.
        if self._scratch_dir and os.path.exists(self._scratch_dir):
            if get_incremental_tests():
                _LOG.warning("Skipping deleting %s", self._scratch_dir)
            else:
                _LOG.debug("Deleting %s", self._scratch_dir)
                io_.delete_dir(self._scratch_dir)

    def create_io_dirs(self) -> None:
        dir_name = self.get_input_dir()
        io_.create_dir(dir_name, incremental=True)
        _LOG.info("Creating dir_name=%s", dir_name)
        dir_name = self.get_output_dir()
        io_.create_dir(dir_name, incremental=True)
        _LOG.info("Creating dir_name=%s", dir_name)

    def get_input_dir(
        self,
        test_class_name: Optional[str] = None,
        test_method_name: Optional[str] = None,
    ) -> str:
        """
        Return the path of the directory storing input data for this test class.

        :return: dir name
        """
        dir_name = (
            self._get_current_path(
                test_class_name=test_class_name, test_method_name=test_method_name
            )
            + "/input"
        )
        return dir_name

    def get_output_dir(self) -> str:
        """
        Return the path of the directory storing output data for this test class.

        :return: dir name
        """
        dir_name = self._get_current_path() + "/output"
        return dir_name

    # TODO(gp): -> get_scratch_dir().
    def get_scratch_space(self) -> str:
        """
        Return the path of the directory storing scratch data for this test class.
        The directory is also created and cleaned up based on whether the
        incremental behavior is enabled or not.

        :return: dir name
        """
        if self._scratch_dir is None:
            # Create the dir on the first invocation on a given test.
            dir_name = os.path.join(self._get_current_path(), "tmp.scratch")
            io_.create_dir(dir_name, incremental=get_incremental_tests())
            self._scratch_dir = dir_name
        return self._scratch_dir

    def assert_equal(self, actual: str, expected: str) -> None:
        dbg.dassert_in(type(actual), (bytes, str))
        dbg.dassert_in(type(expected), (bytes, str))
        #
        dir_name = self._get_current_path()
        _LOG.debug("dir_name=%s", dir_name)
        io_.create_dir(dir_name, incremental=True)
        dbg.dassert_exists(dir_name)
        #
        test_name = self._get_test_name()
        _assert_equal(actual, expected, test_name, dir_name)

    def check_string(
        self, actual: str, fuzzy_match: bool = False, purify_text: bool = True
    ) -> None:
        """
        Check the actual outcome of a test against the expected outcomes
        contained in the file and/or updates the golden reference file with the
        actual outcome.

        :param: purify_text: remove some artifacts (e.g., user names,
            directories, reference to Git client)
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
        # Remove reference from the current purify.
        if purify_text:
            actual = purify_txt_from_client(actual)
        #
        if get_update_tests():
            # Update the test result.
            outcome_updated = False
            file_exists = os.path.exists(file_name)
            if file_exists:
                # The golden outcome exists.
                expected = io_.from_file(file_name)
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
                expected = io_.from_file(file_name)
                test_name = self._get_test_name()
                _assert_equal(
                    actual, expected, test_name, dir_name, fuzzy_match=fuzzy_match
                )
            else:
                # No golden outcome available: save the result in a tmp file.
                tmp_file_name = file_name + ".tmp"
                io_.to_file(tmp_file_name, actual)
                msg = "Can't find golden in %s\nSaved actual outcome in %s" % (
                    file_name,
                    tmp_file_name,
                )
                raise RuntimeError(msg)

    def _get_test_name(self) -> str:
        """
        :return: full test name as class.method.
        """
        return "/%s.%s" % (self.__class__.__name__, self._testMethodName)

    def _get_current_path(
        self,
        test_class_name: Optional[Any] = None,
        test_method_name: Optional[Any] = None,
    ) -> str:
        dir_name = os.path.dirname(inspect.getfile(self.__class__))
        if test_class_name is None:
            test_class_name = self.__class__.__name__
        if test_method_name is None:
            test_method_name = self._testMethodName
        dir_name = dir_name + "/%s.%s" % (test_class_name, test_method_name)
        return dir_name


# #############################################################################
# Notebook testing.
# #############################################################################


def run_notebook(file_name: str, scratch_dir: str) -> None:
    """
    Run jupyter notebook `file_name` using `scratch_dir` as temporary dir
    storing the output.

    Assert if the notebook doesn't complete successfully.
    """
    file_name = os.path.abspath(file_name)
    dbg.dassert_exists(file_name)
    dbg.dassert_exists(scratch_dir)
    # Build command line.
    cmd = []
    cmd.append("cd %s && " % scratch_dir)
    cmd.append("jupyter nbconvert %s" % file_name)
    cmd.append("--execute")
    cmd.append("--to html")
    cmd.append("--ExecutePreprocessor.kernel_name=python")
    # No time-out.
    cmd.append("--ExecutePreprocessor.timeout=-1")
    # Execute.
    cmd_as_str = " ".join(cmd)
    si.system(cmd_as_str, abort_on_error=True)
