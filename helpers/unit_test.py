"""
Import as:

import helpers.unit_test as hut
"""

import inspect
import logging
import os
import pprint
import random
import re
import unittest
from typing import Any, List, Mapping, NoReturn, Optional, Union

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import helpers.dbg as dbg
import helpers.git as git
import helpers.io_ as hio
import helpers.printing as hprint
import helpers.system_interaction as hsyste
import helpers.timer as htimer

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
    title: Optional[str] = None,
    index: bool = False,
    decimals: int = 6,
) -> str:
    """
    Convert DataFrame or Series to string for verifying test results.

    :param df: DataFrame to be verified
    :param n_rows: number of rows in expected output
    :param title: title for test output
    :param decimals: number of decimal points
    :return: string representation of input
    """
    if isinstance(df, pd.Series):
        df = df.to_frame()
    n_rows = n_rows or len(df)
    output = []
    # Add title in the beginning if provided.
    if title is not None:
        output.append(hprint.frame(title))
    # Provide context for full representation of data.
    with pd.option_context(
        "display.max_colwidth",
        int(1e6),
        "display.max_columns",
        None,
        "display.max_rows",
        None,
        "display.precision",
        decimals,
    ):
        # Add top N rows.
        output.append(df.head(n_rows).to_string(index=index))
        output_str = "\n".join(output)
    return output_str


def convert_info_to_string(info: Mapping) -> str:
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
        "display.max_colwidth",
        int(1e6),
        "display.max_columns",
        None,
        "display.max_rows",
        None,
    ):
        output.append(hprint.frame("info"))
        output.append(pprint.pformat(info))
        output_str = "\n".join(output)
    return output_str


def convert_df_to_json_string(
    df: pd.DataFrame,
    n_head: Optional[int] = 10,
    n_tail: Optional[int] = 10,
    columns_order: Optional[List[str]] = None,
) -> str:
    """
    Convert dataframe to pretty-printed json string.

    To select all rows of the dataframe, pass `n_head` as None.

    :param df: dataframe to convert
    :param n_head: number of printed top rows
    :param n_tail: number of printed bottom rows
    :param columns_order: order for the KG columns sort
    :return: dataframe converted to JSON string
    """
    # Append shape of the initial dataframe.
    shape = "original shape=%s" % (df.shape,)
    # Reorder columns.
    if columns_order is not None:
        dbg.dassert_set_eq(columns_order, df.cols)
        df = df[columns_order]
    # Select head.
    if n_head is not None:
        head_df = df.head(n_head)
    else:
        # If no n_head provided, append entire dataframe.
        head_df = df
    # Transform head to json.
    head_json = head_df.to_json(
        orient="index",
        force_ascii=False,
        indent=4,
        default_handler=str,
        date_format="iso",
        date_unit="s",
    )
    if n_tail is not None:
        # Transform tail to json.
        tail = df.tail(n_tail)
        tail_json = tail.to_json(
            orient="index",
            force_ascii=False,
            indent=4,
            default_handler=str,
            date_format="iso",
            date_unit="s",
        )
    else:
        # If no tail specified, append an empty string.
        tail_json = ""
    # Join shape and dataframe to single string.
    output_str = "\n".join([shape, "Head:", head_json, "Tail:", tail_json])
    return output_str


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
        if super_module_path != "/":
            txt = txt.replace(super_module_path, "$GIT_ROOT")
        else:
            # If the git path is `/` then we don't need to do anything.
            pass
    # Replace the current path with `$PWD`
    pwd = os.getcwd()
    txt = txt.replace(pwd, "$PWD")
    # Replace the user name with `$USER_NAME`.
    user_name = hsyste.get_user_name()
    txt = txt.replace(user_name, "$USER_NAME")
    # Remove amp reference, if any.
    txt = remove_amp_references(txt)
    # TODO(gp): Remove conda_sh_path.
    return txt


def diff_files(
    file_name1: str, file_name2: str, tag: Optional[str] = None
) -> NoReturn:
    msg = []
    # Diff to screen.
    _, res = hsyste.system_to_string(
        "echo; sdiff -l -w 150 %s %s" % (file_name1, file_name2),
        abort_on_error=False,
        log_level=logging.DEBUG,
    )
    if tag is not None:
        msg.append("\n" + hprint.frame(tag))
    msg.append(res)
    # Save a script to diff.
    diff_script = "./tmp_diff.sh"
    vimdiff_cmd = "vimdiff %s %s" % (
        os.path.abspath(file_name1),
        os.path.abspath(file_name2),
    )
    hio.to_file(diff_script, vimdiff_cmd)
    cmd = "chmod +x " + diff_script
    hsyste.system(cmd)
    # Report how to diff.
    msg.append("Diff with:")
    msg.append("> " + vimdiff_cmd)
    msg.append("or running:")
    msg.append("> " + diff_script)
    msg_as_str = "\n".join(msg)
    # This is not always shown.
    _LOG.error(msg_as_str)
    raise RuntimeError(msg_as_str)


def diff_strings(string1: str, string2: str, tag: Optional[str] = None) -> None:
    test_dir = "."
    # Save the actual and expected strings to files.
    file_name1 = "%s/tmp.string1.txt" % test_dir
    hio.to_file(file_name1, string1)
    #
    file_name2 = "%s/tmp.string2.txt" % test_dir
    hio.to_file(file_name2, string2)
    #
    if tag is None:
        tag = "string1 vs string2"
    diff_files(file_name1, file_name2, tag)


def diff_df_monotonic(df: pd.DataFrame) -> None:
    if not df.index.is_monotonic_increasing:
        df2 = df.copy()
        df2.sort_index(inplace=True)
        diff_strings(df.to_csv(), df2.to_csv())


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
        _LOG.debug("Using fuzzy match")
        actual_orig = actual
        actual = _remove_spaces(actual)
        expected_orig = expected
        expected = _remove_spaces(expected)
    else:
        actual_orig = actual
        expected_orig = expected
    # Check.
    if expected != actual:
        _LOG.info(
            "%s", "\n" + hprint.frame("Test %s failed" % full_test_name, "=", 80)
        )
        if fuzzy_match:
            # Set the following var to True to print the purified version (e.g.,
            # tables too large).
            print_purified_version = False
            # print_purified_version = True
            if print_purified_version:
                expected = expected_orig
                # actual = actual_orig
        # Print the correct output, like:
        # var = r'""""
        # 2021-02-17 09:30:00-05:00
        # 2021-02-17 10:00:00-05:00
        # 2021-02-17 11:00:00-05:00
        # """
        _LOG.info("\n%s", hprint.frame("Actual variable", "#", 80))
        txt = []
        prefix = "var = r"
        spaces = 0
        # spaces = len(prefix)
        txt.append(prefix + '"""')
        txt.append(hprint.indent(actual_orig, spaces))
        txt.append(hprint.indent('"""', spaces))
        txt = "\n".join(txt)
        print(txt)
        # Save the actual and expected strings to files.
        _LOG.debug("Actual:\n%s", actual)
        act_file_name = "%s/tmp.actual.txt" % test_dir
        hio.to_file(act_file_name, actual)
        #
        _LOG.debug("Expected:\n%s", expected)
        exp_file_name = "%s/tmp.expected.txt" % test_dir
        hio.to_file(exp_file_name, expected)
        #
        tag = "ACTUAL vs EXPECTED"
        diff_files(act_file_name, exp_file_name, tag)


def get_pd_default_values() -> pd._config.config.DictWrapper:
    import copy

    vals = copy.deepcopy(pd.options)
    return vals


def set_pd_default_values() -> None:
    # 'display':
    default_pd_values = {
        "chop_threshold": None,
        "colheader_justify": "right",
        "column_space": 12,
        "date_dayfirst": False,
        "date_yearfirst": False,
        "encoding": "UTF-8",
        "expand_frame_repr": True,
        "float_format": None,
        "html": {"border": 1, "table_schema": False, "use_mathjax": True},
        "large_repr": "truncate",
        "latex": {
            "escape": True,
            "longtable": False,
            "multicolumn": True,
            "multicolumn_format": "l",
            "multirow": False,
            "repr": False,
        },
        "max_categories": 8,
        "max_columns": 20,
        "max_colwidth": 50,
        "max_info_columns": 100,
        "max_info_rows": 1690785,
        "max_rows": 60,
        "max_seq_items": 100,
        "memory_usage": True,
        "min_rows": 10,
        "multi_sparse": True,
        "notebook_repr_html": True,
        "pprint_nest_depth": 3,
        "precision": 6,
        "show_dimensions": "truncate",
        "unicode": {"ambiguous_as_wide": False, "east_asian_width": False},
        "width": 80,
    }
    section = "display"
    for key, new_val in default_pd_values.items():
        if isinstance(new_val, dict):
            continue
        full_key = "%s.%s" % (section, key)
        old_val = pd.get_option(full_key)
        # _LOG.debug("full_key=%s: old_val=%s, new_val=%s", full_key, old_val, new_val)
        if old_val != new_val:
            _LOG.debug(
                "-> Assigning a different value: full_key=%s, old_val=%s, new_val=%s",
                full_key,
                old_val,
                new_val,
            )
        pd.set_option(full_key, new_val)


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
        _LOG.debug("\n%s", hprint.frame(func_name))
        # The base directory is the one including the class under test.
        self.base_dir_name = os.path.dirname(inspect.getfile(self.__class__))
        # Set the default pandas options (see AmpTask1140).
        self.old_pd_options = get_pd_default_values()
        set_pd_default_values()
        # Start the timer to measure the execution time of the test.
        self._timer = htimer.Timer()

    def tearDown(self) -> None:
        # Stop the timer to measure the execution time of the test.
        self._timer.stop()
        print("(%.2f s) " % self._timer.get_total_elapsed(), end="")
        # Recover the original default pandas options.
        pd.options = self.old_pd_options
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
                hio.delete_dir(self._scratch_dir)

    def create_io_dirs(self) -> None:
        dir_name = self.get_input_dir()
        hio.create_dir(dir_name, incremental=True)
        _LOG.info("Creating dir_name=%s", dir_name)
        #
        dir_name = self.get_output_dir()
        hio.create_dir(dir_name, incremental=True)
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
            self._get_current_path( test_class_name, test_method_name ) + "/input"
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
    def get_scratch_space(
        self,
        test_class_name: Optional[Any] = None,
        test_method_name: Optional[Any] = None,
    ) -> str:
        """
        Return the path of the directory storing scratch data for this test
        class. The directory is also created and cleaned up based on whether
        the incremental behavior is enabled or not.

        :return: dir name
        """
        if self._scratch_dir is None:
            # Create the dir on the first invocation on a given test.
            curr_path = self._get_current_path(
                test_class_name=test_class_name, test_method_name=test_method_name
            )
            dir_name = os.path.join(curr_path, "tmp.scratch")
            hio.create_dir(dir_name, incremental=get_incremental_tests())
            self._scratch_dir = dir_name
        return self._scratch_dir

    def assert_equal(
        self, actual: str, expected: str, fuzzy_match: bool = False
    ) -> None:
        """
        Assert if `actual` and `expected` are different and print info about the
        comparison.
        """
        dbg.dassert_in(type(actual), (bytes, str), "actual=%s", str(actual))
        dbg.dassert_in(type(expected), (bytes, str), "expected=%s", str(expected))
        # TODO(gp): Add purify_text.
        # # Remove reference from the current environment.
        # if purify_text:
        #     actual = purify_txt_from_client(actual)
        #
        dir_name = self._get_current_path()
        _LOG.debug("dir_name=%s", dir_name)
        hio.create_dir(dir_name, incremental=True)
        dbg.dassert_exists(dir_name)
        #
        test_name = self._get_test_name()
        _assert_equal(
            actual, expected, test_name, dir_name, fuzzy_match=fuzzy_match
        )

    def check_string(
            self,
            actual: str,
            fuzzy_match: bool = False,
            purify_text: bool = False,
            use_gzip: bool = False,
            tag: str = "test",
    ) -> None:
        """
        Check the actual outcome of a test against the expected outcome
        contained in the file.
        If `--update_outcomes` is used, updates the golden reference file with the
        actual outcome.

        :param: purify_text: remove some artifacts (e.g., user names,
            directories, reference to Git client)
        Raises if there is an error.
        """
        dbg.dassert_in(type(actual), (bytes, str))
        #
        dir_name, file_name = _get_golden_outcome_file_name(tag)
        if use_gzip:
            file_name += ".gz"
        _LOG.debug("file_name=%s", file_name)
        # Remove reference from the current environment.
        if purify_text:
            actual = purify_txt_from_client(actual)

        def _update_outcome(file_name_: str, actual_: str, use_gzip_: bool) -> None:
            hio.create_enclosing_dir(file_name_, incremental=True)
            hio.to_file(file_name_, actual_, use_gzip=use_gzip_)
            # Add to git repo.
            cmd = "git add %s" % file_name_
            rc = hsyste.system(cmd, abort_on_error=False)
            if rc:
                _LOG.warning(
                    "Can't run '%s': you need to add the file manually",
                    cmd,
                )

        if get_update_tests():
            # Determine whether outcome needs to be updated.
            outcome_updated = False
            file_exists = os.path.exists(file_name)
            if file_exists:
                expected = hio.from_file(file_name, use_gzip=use_gzip)
                if expected != actual:
                    outcome_updated = True
            else:
                # The golden outcome doesn't exist.
                outcome_updated = True
            _LOG.debug("outcome_updated=%s", outcome_updated)
            if outcome_updated:
                # Update the golden outcome.
                _LOG.warning("Golden outcome updated in '%s'", file_name)
                _update_outcome(file_name, actual, use_gzip)
        else:
            # Check the test result.
            if os.path.exists(file_name):
                # Golden outcome is available: check the actual outcome against
                # the golden outcome.
                expected = hio.from_file(file_name, use_gzip=use_gzip)
                test_name = self._get_test_name()
                _assert_equal(
                    actual, expected, test_name, dir_name, fuzzy_match=fuzzy_match
                )
            else:
                # No golden outcome available: save the result.
                _LOG.warning("Can't find golden outcome file '%s': updating it",
                             file_name)
                _update_outcome(file_name, actual, use_gzip)

    def _get_golden_outcome_file_name(self, tag: str) -> Tuple[str, str]:
        dir_name = self._get_current_path()
        _LOG.debug("dir_name=%s", dir_name)
        hio.create_dir(dir_name, incremental=True)
        dbg.dassert_exists(dir_name)
        # Get the expected outcome.
        file_name = self.get_output_dir() + f"/{tag}.txt"
        return dir_name, file_name

    def check_dataframe(
            self,
            actual: pd.DataFrame,
            err_threshold: float = 0.05,
            tag: str = "test_df",
    ) -> None:
        """
        Like check_string() but for pandas dataframes, instead of strings.
        """
        dbg.dassert_isinstance(actual, pd.DataFrame)
        #
        dir_name, file_name = _get_golden_outcome_file_name(tag)
        _LOG.debug("file_name=%s", file_name)

        def _compare_outcome(file_name_: str, actual_: pd.DataFrame, err_threshold_: float) -> bool:
            dbg.dassert_lte(0, err_threshold_)
            dbg.dassert_lte(err_threshold_, 1.0)
            expected = pd.DataFrame.read_csv(file_name_)
            ret = True
            if actual_.columns != expected.columns:
                _LOG.debug("Columns are different: %s != %s", str(actual_.columns), str(expected.columns))
                ret = False
            is_close = np.allclose(actual_, expected, rtol=err_threshold_, equal_nan=True)
            if not is_close:
                _LOG.debug("Dataframes are not close")
                ret = False
            _LOG.debug("ret=%s", ret)
            return ret

        def _update_outcome(file_name_: str, actual_: pd.DataFrame) -> None:
            hio.create_enclosing_dir(file_name_, incremental=True)
            actual_.to_csv(file_name)
            # Add to git.
            cmd = "git add %s" % file_name_
            rc = hsyste.system(cmd, abort_on_error=False)
            if rc:
                _LOG.warning(
                    "Can't run '%s': you need to add the file manually",
                    cmd,
                )

        file_exists = os.path.exists(file_name)
        if get_update_tests():
            # Determine whether outcome needs to be updated.
            outcome_updated = False
            if file_exists:
                is_equal = _compare_outcome(file_name, actual, err_threshold)
                if not is_equal:
                    outcome_updated = True
            else:
                # The golden outcome doesn't exist.
                outcome_updated = True
            _LOG.debug("outcome_updated=%s", outcome_updated)
            if outcome_updated:
                # Update the golden outcome.
                _LOG.warning("Golden outcome updated in '%s'", file_name)
                _update_outcome(file_name, actual)
        else:
            # Check the test result.
            if file_exists:
                # Golden outcome is available: check the actual outcome against
                # the golden outcome.
                is_equal = _compare_outcome(file_name, actual, err_threshold)
                if is_equal:
                    test_name = self._get_test_name()
                    _assert_equal(
                        str(actual), str(expected), test_name, dir_name,
                    )

            else:
                # No golden outcome available: save the result.
                _LOG.warning("Can't find golden outcome file '%s': updating it",
                             file_name)
                _update_outcome(file_name, actual)

    def _get_test_name(self) -> str:
        """
        Return the full test name as `/class.method`.
        """
        # TODO(gp): Why do we need the leading "/".
        return "/%s.%s" % (self.__class__.__name__, self._testMethodName)

    def _get_current_path(
        self,
        test_class_name: Optional[str] = None,
        test_method_name: Optional[str] = None,
    ) -> str:
        """
        Return the name of the directory containing the input / output data
        (e.g., ./core/dataflow/test/TestContinuousSarimaxModel.test_compare)
        """
        if test_class_name is None:
            test_class_name = self.__class__.__name__
        if test_method_name is None:
            test_method_name = self._testMethodName
        # E.g., ./core/dataflow/test/TestContinuousSarimaxModel.test_compare
        dir_name = self.base_dir_name + "/%s.%s" % (test_class_name, test_method_name)
        return dir_name


# #############################################################################
# Notebook testing.
# #############################################################################


def run_notebook(
    file_name: str,
    scratch_dir: str,
    config_builder: Optional[str] = None,
    idx: int = 0,
) -> None:
    """
    Run jupyter notebook.

    `core.config_builders.get_config_from_env()` supports passing in a config
    only through a path to a config builder function that returns a list of
    configs, and a config index from that list.

    Assert if the notebook doesn't complete successfully.

    :param file_name: path to the notebook to run. If this is a .py file,
        convert to .ipynb first
    :param scratch_dir: temporary dir storing the output
    :param config_builder: path to config builder function that returns a list
        of configs
    :param idx: index of target config in the config list
    """
    file_name = os.path.abspath(file_name)
    dbg.dassert_exists(file_name)
    dbg.dassert_exists(scratch_dir)
    # Build command line.
    cmd = []
    # Convert .py file into .ipynb if needed.
    root, ext = os.path.splitext(file_name)
    if ext == ".ipynb":
        notebook_name = file_name
    elif ext == ".py":
        cmd.append(f"jupytext --update --to notebook {file_name}; ")
        notebook_name = f"{root}.ipynb"
    else:
        raise ValueError(f"Unsupported file format for `file_name`='{file_name}'")
    # Export config variables.
    if config_builder is not None:
        cmd.append(f'export __CONFIG_BUILDER__="{config_builder}"; ')
        cmd.append(f'export __CONFIG_IDX__="{idx}"; ')
        cmd.append(f'export __CONFIG_DST_DIR__="{scratch_dir}" ;')
    # Execute notebook.
    cmd.append("cd %s && " % scratch_dir)
    cmd.append("jupyter nbconvert %s" % notebook_name)
    cmd.append("--execute")
    cmd.append("--to html")
    cmd.append("--ExecutePreprocessor.kernel_name=python")
    # No time-out.
    cmd.append("--ExecutePreprocessor.timeout=-1")
    # Execute.
    cmd_as_str = " ".join(cmd)
    hsyste.system(cmd_as_str, abort_on_error=True)
