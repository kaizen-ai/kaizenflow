"""
Import as:

import helpers.unit_test as hut
"""

import glob
import inspect
import logging
import os
import pprint
import random
import re
import unittest
from typing import Any, List, Mapping, Optional, Tuple, Union

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import helpers.dbg as dbg
import helpers.git as git
import helpers.io_ as hio
import helpers.printing as hprint
import helpers.system_interaction as hsinte
import helpers.timer as htimer

_LOG = logging.getLogger(__name__)
_LOG.setLevel(logging.INFO)

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


# TODO(gp): Use https://stackoverflow.com/questions/25188119
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


def get_dir_signature(dir_name: str, num_lines: Optional[int] = None) -> str:
    """
    Compute a string with the content of files in dir_name.

    :param num_lines: number of lines to print for each file
    """
    # Find all the files under `dir_name`.
    _LOG.debug("dir_name=%s", dir_name)
    dbg.dassert_exists(dir_name)
    file_names = glob.glob(os.path.join(dir_name, "*"), recursive=True)
    file_names = sorted(file_names)
    #
    txt: List[str] = []
    txt.append("len(file_names)=%s" % len(file_names))
    txt.append("file_names=%s" % ", ".join(file_names))
    # Scan the files.
    for file_name in file_names:
        _LOG.debug("file_name=%s", file_name)
        txt.append("# " + file_name)
        # Read file.
        txt_tmp = hio.from_file(file_name)
        # This seems unstable on different systems.
        # txt.append("num_chars=%s" % len(txt_tmp))
        txt_tmp = txt_tmp.split("\n")
        # Filter lines, if needed.
        txt.append("num_lines=%s" % len(txt_tmp))
        if num_lines is not None:
            dbg.dassert_lte(1, num_lines)
            txt_tmp = txt_tmp[:num_lines]
        txt.append("'''\n" + "\n".join(txt_tmp) + "\n'''")
    # Concat.
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
    user_name = hsinte.get_user_name()
    txt = txt.replace(user_name, "$USER_NAME")
    # Remove amp reference, if any.
    txt = remove_amp_references(txt)
    # TODO(gp): Remove conda_sh_path.
    return txt


def diff_files(
    file_name1: str,
    file_name2: str,
    tag: Optional[str] = None,
    abort_on_exit: bool = True,
    dst_dir: str = ".",
    error_msg: str = "",
) -> None:
    """
    Compare the passed filenames and create script to compare them with
    vimdiff.

    :param tag: add a banner the tag
    :param abort_on_exit: whether to assert or not
    :param dst_dir: dir where to save the comparing script
    """
    _LOG.debug(hprint.to_str("tag abort_on_exit dst_dir"))
    file_name1 = os.path.relpath(file_name1, os.getcwd())
    file_name2 = os.path.relpath(file_name2, os.getcwd())
    msg = []
    # Add tag.
    if tag is not None:
        msg.append("\n" + hprint.frame(tag, "-"))
    # Diff to screen.
    _, res = hsinte.system_to_string(
        "echo; sdiff --expand-tabs -l -w 150 %s %s" % (file_name1, file_name2),
        abort_on_error=False,
        log_level=logging.DEBUG,
    )
    msg.append(res)
    # Save a script to diff.
    diff_script = os.path.join(dst_dir, "tmp_diff.sh")
    vimdiff_cmd = "vimdiff %s %s" % (file_name1, file_name2)
    hio.to_file(diff_script, vimdiff_cmd)
    cmd = "chmod +x " + diff_script
    hsinte.system(cmd)
    # Report how to diff.
    msg.append("Diff with:")
    msg.append("> " + vimdiff_cmd)
    msg.append("or running:")
    msg.append("> " + diff_script)
    msg_as_str = "\n".join(msg)
    # Append also error_msg to the current message.
    if error_msg:
        msg_as_str += "\n" + error_msg
    if abort_on_exit:
        raise RuntimeError(msg_as_str)
    _LOG.error(msg_as_str)


def diff_strings(
    string1: str,
    string2: str,
    tag: Optional[str] = None,
    abort_on_exit: bool = True,
    dst_dir: str = ".",
) -> None:
    """
    Compare two strings using the diff_files() flow by creating a script to
    compare with vimdiff.

    :param dst_dir: where to save the intermediatary files
    """
    _LOG.debug(hprint.to_str("tag abort_on_exit dst_dir"))
    # Save the actual and expected strings to files.
    file_name1 = "%s/tmp.string1.txt" % dst_dir
    hio.to_file(file_name1, string1)
    #
    file_name2 = "%s/tmp.string2.txt" % dst_dir
    hio.to_file(file_name2, string2)
    # Compare with diff_files.
    if tag is None:
        tag = "string1 vs string2"
    diff_files(
        file_name1,
        file_name2,
        tag=tag,
        abort_on_exit=abort_on_exit,
        dst_dir=dst_dir,
    )


def diff_df_monotonic(
    df: pd.DataFrame,
    tag: Optional[str] = None,
    abort_on_exit: bool = True,
    dst_dir: str = ".",
) -> None:
    """
    Check for a dataframe to be monotonic using the vimdiff flow from
    diff_files().
    """
    _LOG.debug(hprint.to_str("abort_on_exit dst_dir"))
    if not df.index.is_monotonic_increasing:
        df2 = df.copy()
        df2.sort_index(inplace=True)
        diff_strings(
            df.to_csv(),
            df2.to_csv(),
            tag=tag,
            abort_on_exit=abort_on_exit,
            dst_dir=dst_dir,
        )


# #############################################################################


# pylint: disable=protected-access
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
        if old_val != new_val:
            _LOG.debug(
                "-> Assigning a different value: full_key=%s, "
                "old_val=%s, new_val=%s",
                full_key,
                old_val,
                new_val,
            )
        pd.set_option(full_key, new_val)


# #############################################################################


def _remove_spaces(obj: Any) -> str:
    """
    Remove spaces to implement fuzzy matching.
    """
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


def _to_pretty_string(obj: str) -> str:
    if isinstance(obj, dict):
        ret = pprint.pformat(obj)
    else:
        ret = str(obj)
    ret = ret.rstrip("\n")
    return ret


def _assert_equal(
    actual: str,
    expected: str,
    full_test_name: str,
    test_dir: str,
    fuzzy_match: bool = False,
    abort_on_error: bool = True,
    dst_dir: str = ".",
    error_msg: str = "",
) -> bool:
    """
    Implement a better version of self.assertEqual() that reports mismatching
    strings with sdiff and save them to files for further analysis with
    vimdiff.

    :param fuzzy_match: ignore differences in spaces and end of lines (see
      `_remove_spaces`)
    :return: whether `actual` and `expected` are equal, if `abort_on_error` is False
    """
    _LOG.debug(
        hprint.to_str(
            "full_test_name test_dir fuzzy_match abort_on_error dst_dir"
        )
    )
    # Convert to strings.
    actual = _to_pretty_string(actual)
    expected = _to_pretty_string(expected)
    # Fuzzy match, if needed.
    if fuzzy_match:
        _LOG.debug("# Using fuzzy match")
        actual_orig = actual
        actual = _remove_spaces(actual)
        expected_orig = expected
        expected = _remove_spaces(expected)
    else:
        actual_orig = actual
        expected_orig = expected
    # Check.
    _LOG.debug("act=\n'%s'", actual)
    _LOG.debug("exp=\n'%s'", expected)
    is_equal = expected == actual
    if not is_equal:
        _LOG.error(
            "%s",
            "\n" + hprint.frame("Test '%s' failed" % full_test_name, "=", 80),
        )
        if fuzzy_match:
            # Set True to print the purified version (e.g., tables too large).
            if True:
                expected = expected_orig
                # actual = actual_orig
        # Print the correct output, like:
        #   exp = r'""""
        #   2021-02-17 09:30:00-05:00
        #   2021-02-17 10:00:00-05:00
        #   2021-02-17 11:00:00-05:00
        #   """
        txt = []
        txt.append(hprint.frame("The expected variable should be", "-"))
        prefix = "exp = r"
        spaces = 0
        # spaces = len(prefix)
        txt.append(prefix + '"""')
        txt.append(hprint.indent(actual_orig, spaces))
        txt.append(hprint.indent('"""', spaces))
        txt = "\n".join(txt)
        error_msg += txt
        # Save the actual and expected strings to files.
        _LOG.debug("Actual:\n'%s'", actual)
        act_file_name = "%s/tmp.actual.txt" % test_dir
        hio.to_file(act_file_name, actual)
        #
        _LOG.debug("Expected:\n'%s'", expected)
        exp_file_name = "%s/tmp.expected.txt" % test_dir
        hio.to_file(exp_file_name, expected)
        #
        tag = "ACTUAL vs EXPECTED"
        diff_files(
            act_file_name,
            exp_file_name,
            tag=tag,
            abort_on_exit=abort_on_error,
            dst_dir=dst_dir,
            error_msg=error_msg,
        )
    _LOG.debug("-> is_equal=%s", is_equal)
    return is_equal


# #############################################################################


class TestCase(unittest.TestCase):
    """
    Add some functions to compare actual results to a golden outcome.
    """

    def setUp(self) -> None:
        # Print banner to signal the start of a new test.
        func_name = "%s.%s" % (self.__class__.__name__, self._testMethodName)
        _LOG.debug("\n%s", hprint.frame(func_name))
        # Set the random seed.
        random.seed(20000101)
        np.random.seed(20000101)
        # Disable matplotlib plotting by overwriting the `show` function.
        plt.show = lambda: 0
        # Name of the dir with artifacts for this test.
        self._scratch_dir: Optional[str] = None
        # The base directory is the one including the class under test.
        self._base_dir_name = os.path.dirname(inspect.getfile(self.__class__))
        _LOG.debug("base_dir_name=%s", self._base_dir_name)
        # Store whether a test needs to be updated or not.
        self._update_tests = get_update_tests()
        self._overriden_update_tests = False
        # Store whether the golden outcome of this test was updated.
        self._test_was_updated = False
        # Store whether the output files need to be added to git.
        self._git_add = True
        # Error message printed when comparing actual and expected outcome.
        self._error_msg = ""
        # Set the default pandas options (see AmpTask1140).
        self._old_pd_options = get_pd_default_values()
        set_pd_default_values()
        # Start the timer to measure the execution time of the test.
        self._timer = htimer.Timer()

    def tearDown(self) -> None:
        # Stop the timer to measure the execution time of the test.
        self._timer.stop()
        print("(%.2f s) " % self._timer.get_total_elapsed(), end="")
        # Report if the test was updated
        if self._test_was_updated:
            if not self._overriden_update_tests:
                print(
                    "("
                    + hprint.color_highlight("WARNING", "yellow")
                    + ": Test was updated) ",
                    end="",
                )
            else:
                # We forced an update from the unit test itself, so no need
                # to report an update.
                pass
        # Recover the original default pandas options.
        pd.options = self._old_pd_options
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

    def set_base_dir_name(self, base_dir_name: str) -> None:
        """
        Set the base directory for the input, output, and scratch directories.

        This is used to override the standard location of the base
        directory which is close to the class under test.
        """
        self._base_dir_name = base_dir_name
        _LOG.debug("Setting base_dir_name to '%s'", self._base_dir_name)
        hio.create_dir(self._base_dir_name, incremental=True)

    def mock_update_tests(self) -> None:
        """
        When unit testing the unit test framework we want to test updating the
        golden outcome.
        """
        self._update_tests = True
        self._overriden_update_tests = True
        self._git_add = False

    def get_input_dir(
        self,
        test_class_name: Optional[str] = None,
        test_method_name: Optional[str] = None,
    ) -> str:
        """
        Return the path of the directory storing input data for this test
        class.

        :return: dir name
        """
        dir_name = os.path.join(
            self._get_current_path(test_class_name, test_method_name), "input"
        )
        return dir_name

    def get_output_dir(self) -> str:
        """
        Return the path of the directory storing output data for this test
        class.

        :return: dir name
        """
        dir_name = os.path.join(self._get_current_path(), "output")
        return dir_name

    # TODO(gp): -> get_scratch_dir().
    def get_scratch_space(
        self,
        test_class_name: Optional[str] = None,
        test_method_name: Optional[str] = None,
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
        self,
        actual: str,
        expected: str,
        fuzzy_match: bool = False,
        abort_on_error: bool = True,
        dst_dir: str = ".",
    ) -> bool:
        """
        Assert if `actual` and `expected` are different and print info about
        the comparison.
        """
        _LOG.debug(hprint.to_str("fuzzy_match abort_on_error dst_dir"))
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
        is_equal = _assert_equal(
            actual,
            expected,
            test_name,
            dir_name,
            fuzzy_match=fuzzy_match,
            abort_on_error=abort_on_error,
            dst_dir=dst_dir,
        )
        return is_equal

    def check_string(
        self,
        actual: str,
        fuzzy_match: bool = False,
        purify_text: bool = False,
        use_gzip: bool = False,
        tag: str = "test",
        abort_on_error: bool = True,
    ) -> Tuple[bool, bool, Optional[bool]]:
        """
        Check the actual outcome of a test against the expected outcome
        contained in the file. If `--update_outcomes` is used, updates the
        golden reference file with the actual outcome.

        :param: purify_text: remove some artifacts (e.g., user names,
            directories, reference to Git client)
        :return: outcome_updated, file_exists, is_equal
        :raises: RuntimeError if there is an error unless `about_on_error` is
            False, which should be used only for unit testing
        """
        _LOG.debug(hprint.to_str("fuzzy_match purify_text abort_on_error"))
        dbg.dassert_in(type(actual), (bytes, str))
        #
        dir_name, file_name = self._get_golden_outcome_file_name(tag)
        if use_gzip:
            file_name += ".gz"
        _LOG.debug("file_name=%s", file_name)
        # Remove reference from the current environment.
        if purify_text:
            _LOG.debug("Purifying actual")
            actual = purify_txt_from_client(actual)
        _LOG.debug("actual=\n%s", actual)
        outcome_updated = False
        file_exists = os.path.exists(file_name)
        _LOG.debug("file_exists=%s", file_exists)
        is_equal: Optional[bool] = None
        if self._update_tests:
            _LOG.debug("Update golden outcomes")
            # Determine whether outcome needs to be updated.
            if file_exists:
                expected = hio.from_file(file_name, use_gzip=use_gzip)
                is_equal = expected == actual
                if not is_equal:
                    outcome_updated = True
            else:
                # The golden outcome doesn't exist.
                outcome_updated = True
            _LOG.debug("outcome_updated=%s", outcome_updated)
            if outcome_updated:
                # Update the golden outcome.
                self._check_string_update_outcome(file_name, actual, use_gzip)
        else:
            # Check the test result.
            _LOG.debug("Check golden outcomes")
            if file_exists:
                # Golden outcome is available: check the actual outcome against
                # the golden outcome.
                expected = hio.from_file(file_name, use_gzip=use_gzip)
                test_name = self._get_test_name()
                is_equal = _assert_equal(
                    actual,
                    expected,
                    test_name,
                    dir_name,
                    fuzzy_match=fuzzy_match,
                    abort_on_error=abort_on_error,
                )
            else:
                # No golden outcome available: save the result.
                _LOG.warning(
                    "Can't find golden outcome file '%s': updating it", file_name
                )
                outcome_updated = True
                self._check_string_update_outcome(file_name, actual, use_gzip)
                is_equal = None
        self._test_was_updated = outcome_updated
        _LOG.debug(hprint.to_str("outcome_updated file_exists is_equal"))
        return outcome_updated, file_exists, is_equal

    def check_dataframe(
        self,
        actual: pd.DataFrame,
        err_threshold: float = 0.05,
        tag: str = "test_df",
        abort_on_error: bool = True,
    ) -> Tuple[bool, bool, Optional[bool]]:
        """
        Like check_string() but for pandas dataframes, instead of strings.

        :return: outcome_updated, file_exists, is_equal
        :raises: RuntimeError if there is an error unless `about_on_error` is
            False, which should be used only for unit testing
        """
        _LOG.debug(hprint.to_str("err_threshold tag abort_on_error"))
        dbg.dassert_isinstance(actual, pd.DataFrame)
        #
        dir_name, file_name = self._get_golden_outcome_file_name(tag)
        _LOG.debug("file_name=%s", file_name)
        outcome_updated = False
        file_exists = os.path.exists(file_name)
        _LOG.debug(hprint.to_str("file_exists"))
        is_equal: Optional[bool] = None
        if self._update_tests:
            _LOG.debug("Update golden outcomes")
            # Determine whether outcome needs to be updated.
            if file_exists:
                is_equal, _ = self._check_df_compare_outcome(
                    file_name, actual, err_threshold
                )
                _LOG.debug(hprint.to_str("is_equal"))
                if not is_equal:
                    outcome_updated = True
            else:
                # The golden outcome doesn't exist.
                outcome_updated = True
            _LOG.debug("outcome_updated=%s", outcome_updated)
            if outcome_updated:
                # Update the golden outcome.
                self._check_df_update_outcome(file_name, actual)
        else:
            # Check the test result.
            if file_exists:
                # Golden outcome is available: check the actual outcome against
                # the golden outcome.
                is_equal, expected = self._check_df_compare_outcome(
                    file_name, actual, err_threshold
                )
                # If not equal, report debug information.
                if not is_equal:
                    test_name = self._get_test_name()
                    _assert_equal(
                        str(actual),
                        str(expected),
                        test_name,
                        dir_name,
                        fuzzy_match=False,
                        abort_on_error=abort_on_error,
                        error_msg=self._error_msg,
                    )
            else:
                # No golden outcome available: save the result.
                _LOG.warning(
                    "Can't find golden outcome file '%s': updating it", file_name
                )
                outcome_updated = True
                self._check_df_update_outcome(file_name, actual)
                is_equal = None
        self._test_was_updated = outcome_updated
        _LOG.debug(hprint.to_str("outcome_updated file_exists is_equal"))
        return outcome_updated, file_exists, is_equal

    # #########################################################################

    def _git_add_file(self, file_name: str) -> None:
        """
        Add to git repo `file_name`, if needed.
        """
        if self._git_add:
            cmd = "git add %s; git add -u %s" % (file_name, file_name)
            _LOG.debug("> %s", cmd)
            rc = hsinte.system(cmd, abort_on_error=False)
            if rc:
                _LOG.warning(
                    "Can't run '%s': you need to add the file manually",
                    cmd,
                )

    def _check_string_update_outcome(
        self, file_name: str, actual: str, use_gzip: bool
    ) -> None:
        _LOG.debug(hprint.to_str("file_name"))
        hio.to_file(file_name, actual, use_gzip=use_gzip)
        # Add to git repo.
        self._git_add_file(file_name)

    # #########################################################################

    def _check_df_update_outcome(
        self, file_name: str, actual: pd.DataFrame
    ) -> None:
        _LOG.debug(hprint.to_str("file_name"))
        hio.create_enclosing_dir(file_name)
        actual.to_csv(file_name)
        # Add to git repo.
        self._git_add_file(file_name)

    def _check_df_compare_outcome(
        self, file_name: str, actual: pd.DataFrame, err_threshold: float
    ) -> Tuple[bool, pd.DataFrame]:
        _LOG.debug(hprint.to_str("file_name"))
        _LOG.debug("actual_=\n%s", actual)
        dbg.dassert_lte(0, err_threshold)
        dbg.dassert_lte(err_threshold, 1.0)
        # Load the expected df from file.
        expected = pd.read_csv(file_name, index_col=0)
        _LOG.debug("expected=\n%s", expected)
        dbg.dassert_isinstance(expected, pd.DataFrame)
        ret = True
        # Compare columns.
        if actual.columns.tolist() != expected.columns.tolist():
            msg = "Columns are different:\n%s\n%s" % (
                str(actual.columns),
                str(expected.columns),
            )
            self._to_error(msg)
            ret = False
        # Compare the values.
        _LOG.debug("actual.shape=%s", str(actual.shape))
        _LOG.debug("expected.shape=%s", str(expected.shape))
        # From https://numpy.org/doc/stable/reference/generated/numpy.allclose.html
        # absolute(a - b) <= (atol + rtol * absolute(b))
        # absolute(a - b) / absolute(b)) <= rtol
        is_close = np.allclose(
            actual, expected, rtol=err_threshold, equal_nan=True
        )
        if not is_close:
            _LOG.error("Dataframe values are not close")
            if actual.shape == expected.shape:
                close_mask = np.isclose(actual, expected, equal_nan=True)
                #
                msg = "actual=\n%s" % actual
                self._to_error(msg)
                #
                msg = "expected=\n%s" % expected
                self._to_error(msg)
                #
                actual_masked = np.where(close_mask, np.nan, actual)
                msg = "actual_masked=\n%s" % actual_masked
                self._to_error(msg)
                #
                expected_masked = np.where(close_mask, np.nan, expected)
                msg = "expected_masked=\n%s" % expected_masked
                self._to_error(msg)
                #
                err = np.abs((actual_masked - expected_masked) / expected_masked)
                msg = "err=\n%s" % err
                self._to_error(msg)
                max_err = np.nanmax(np.nanmax(err))
                msg = "max_err=%.3f" % max_err
                self._to_error(msg)
            else:
                msg = (
                    "Shapes are different:\n"
                    "actual.shape=%s\n"
                    "expected.shape=%s" % (str(actual.shape), str(expected.shape))
                )
                self._to_error(msg)
            ret = False
        _LOG.debug("ret=%s", ret)
        return ret, expected

    # #########################################################################

    def _get_golden_outcome_file_name(self, tag: str) -> Tuple[str, str]:
        dir_name = self._get_current_path()
        _LOG.debug("dir_name=%s", dir_name)
        hio.create_dir(dir_name, incremental=True)
        dbg.dassert_exists(dir_name)
        # Get the expected outcome.
        file_name = self.get_output_dir() + f"/{tag}.txt"
        return dir_name, file_name

    def _get_test_name(self) -> str:
        """
        Return the full test name as `class.method`.
        """
        return "%s.%s" % (self.__class__.__name__, self._testMethodName)

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
        dir_name = self._base_dir_name + "/%s.%s" % (
            test_class_name,
            test_method_name,
        )
        return dir_name

    def _to_error(self, msg: str) -> None:
        self._error_msg += msg + "\n"
        _LOG.error(msg)


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
    hsinte.system(cmd_as_str, abort_on_error=True)
