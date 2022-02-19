"""
Import as:

import helpers.hdbg as hdbg
"""

# This module should not depend on anything else than Python standard modules.

import logging
import os
import pprint
import sys
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Type, Union

_LOG = logging.getLogger(__name__)


# TODO(gp): Make these generate from MAPPING below.
INFO = "\033[36mINFO\033[0m"
WARNING = "\033[33mWARNING\033[0m"
ERROR = "\033[31mERROR\033[0m"


# #############################################################################
# dfatal.
# #############################################################################

# Copied from printing.py to avoid cyclical dependencies.


def _line(chars: str = "#", num_cols: int = 80) -> str:
    line_ = chars * num_cols + "\n"
    return line_


def _frame(x: str, chars: str = "#", num_cols: int = 80) -> str:
    """
    Return a string with a frame of num_cols chars around the object x.

    :param x: object to print through str()
    :param num_cols: number
    """
    line_ = _line(chars=chars, num_cols=num_cols)
    ret = ""
    ret += line_
    ret += str(x) + "\n"
    ret += line_
    return ret


# End of copy.


def dfatal(message: str, assertion_type: Optional[Any] = None) -> None:
    """
    Print an error message and exits.
    """
    ret = ""
    message = str(message)
    ret = "\n" + _frame(message, "#", 80)
    if assertion_type is None:
        assertion_type = AssertionError
    raise assertion_type(ret)


# #############################################################################
# dassert.
# #############################################################################

# TODO(gp): Would be nice to have a way to disable the assertions in certain
#  builds, or at least know how much time is spent in the assertions.
#  To disable we could have a fake_dbg.py that has all `dassert_*`, `logging`
#   defined as `lambda x: 0`.


# INVARIANTS:
# - `dassert_COND()` checks that COND is true, and raises if COND is False
# - For this reason the condition inside the `dassert` is typically in the form
#   `if not (...):`, even this might annoy the linter or look weird
# - The parameter `only_warning` is to report a problem but keep going.
#   This can be used (sparingly) for production when we want to be aware of
#   certain conditions without aborting.


def _to_msg(msg: Optional[str], *args: Any) -> str:
    """
    Format error message `msg` using the params in `args`, like `msg % args`.
    """
    if msg is None:
        # If there is no message, we should have no arguments to format.
        assert not args, "args=%s" % str(args)
        res = ""
    else:
        try:
            res = msg % args
        except TypeError as e:
            # The arguments didn't match the format string: report error and
            # print the result somehow.
            res = "Caught assertion while formatting message:\n'%s'" % str(e)
            _LOG.warning(res)
            res += "\n" + msg + " " + " ".join(map(str, args))
        # res = "(" + res + ") "
    return res


def _dfatal(
    txt: Union[str, Iterable[str]],
    msg: Optional[str],
    *args: Any,
    only_warning: bool = False,
) -> None:
    """
    Abort execution.

    :param only_warning: issue a warning instead of aborting
    """
    dfatal_txt = "* Failed assertion *\n"
    # TODO(gp): This should be an iterable.
    if isinstance(txt, list):
        dfatal_txt += "\n".join(txt)
    else:
        dfatal_txt += str(txt)
    msg = _to_msg(msg, *args)
    if msg:
        if not dfatal_txt.endswith("\n"):
            dfatal_txt += "\n"
        dfatal_txt += msg
    if only_warning:
        # Only warn.
        dfatal_txt += "\nContinuing as per user request with only_warning=True"
        _LOG.warning(dfatal_txt)
    else:
        # Abort.
        dfatal(dfatal_txt)


def dassert(
    cond: Any,
    msg: Optional[str] = None,
    *args: Any,
    only_warning: bool = False,
) -> None:
    # Handle the somehow frequent case of using `dassert` instead of another
    # one, e.g., `dassert(y, list)`
    if msg is not None:
        assert isinstance(
            msg, str
        ), f"You passed '{msg}' or type '{type(msg)}' instead of str"
    if not cond:
        txt = "cond=%s" % cond
        _dfatal(txt, msg, *args, only_warning=only_warning)


def dassert_eq(
    val1: Any,
    val2: Any,
    msg: Optional[str] = None,
    *args: Any,
    only_warning: bool = False,
) -> None:
    cond = val1 == val2
    if not cond:
        txt = "'%s'\n==\n'%s'" % (val1, val2)
        _dfatal(txt, msg, *args, only_warning=only_warning)


def dassert_ne(
    val1: Any,
    val2: Any,
    msg: Optional[str] = None,
    *args: Any,
    only_warning: bool = False,
) -> None:
    cond = val1 != val2
    if not cond:
        txt = "'%s'\n!=\n'%s'" % (val1, val2)
        _dfatal(txt, msg, *args, only_warning=only_warning)


def dassert_imply(
    val1: Any,
    val2: Any,
    msg: Optional[str] = None,
    *args: Any,
    only_warning: bool = False,
) -> None:
    cond = not val1 or val2
    if not cond:
        txt = "'%s' implies '%s'" % (val1, val2)
        _dfatal(txt, msg, *args, only_warning=only_warning)


# Comparison related.


def dassert_lt(
    val1: Any,
    val2: Any,
    msg: Optional[str] = None,
    *args: Any,
    only_warning: bool = False,
) -> None:
    cond = val1 < val2
    if not cond:
        txt = "%s < %s" % (val1, val2)
        _dfatal(txt, msg, *args, only_warning=only_warning)


def dassert_lte(
    val1: Any,
    val2: Any,
    msg: Optional[str] = None,
    *args: Any,
    only_warning: bool = False,
) -> None:
    cond = val1 <= val2
    if not cond:
        txt = "%s <= %s" % (val1, val2)
        _dfatal(txt, msg, *args, only_warning=only_warning)


def dassert_lgt(
    lower_bound: float,
    x: float,
    upper_bound: float,
    lower_bound_closed: bool,
    upper_bound_closed: bool,
    msg: Optional[str] = None,
    *args: Any,
    only_warning: bool = False,
) -> None:
    """
    Assert that `lower_bound <= x <= upper_bound`.

    :param lower_bound_closed, upper_bound_closed: control the
        open-ness/close-ness of the interval extremes.
    """
    # `lower_bound <= or < x`.
    if lower_bound_closed:
        dassert_lte(lower_bound, x, msg, *args, only_warning=only_warning)
    else:
        dassert_lt(lower_bound, x, msg, *args, only_warning=only_warning)
    # `x <= or < upper_bound`.
    if upper_bound_closed:
        dassert_lte(x, upper_bound, msg, *args, only_warning=only_warning)
    else:
        dassert_lt(x, upper_bound, msg, *args, only_warning=only_warning)


def dassert_is_proportion(
    x: float, msg: Optional[str] = None, *args: Any, only_warning: bool = False
) -> None:
    """
    Assert that `0 <= x <= 1`.
    """
    lower_bound_closed = True
    upper_bound_closed = True
    dassert_lgt(
        0,
        x,
        1,
        lower_bound_closed,
        upper_bound_closed,
        msg,
        *args,
        only_warning=only_warning,
    )


# Membership.


def dassert_in(
    value: Any,
    valid_values: Any,
    msg: Optional[str] = None,
    *args: Any,
    only_warning: bool = False,
) -> None:
    cond = value in valid_values
    if not cond:
        txt = "'%s' in '%s'" % (value, valid_values)
        _dfatal(txt, msg, *args, only_warning=only_warning)


def dassert_not_in(
    value: Any,
    valid_values: Iterable[Any],
    msg: Optional[str] = None,
    *args: Any,
    only_warning: bool = False,
) -> None:
    cond = value not in valid_values
    if not cond:
        txt = "'%s' not in '%s'" % (value, valid_values)
        _dfatal(txt, msg, *args, only_warning=only_warning)


# Type related.


def dassert_is(
    val1: Optional[str],
    val2: Optional[Any],
    msg: Optional[str] = None,
    *args: Any,
    only_warning: bool = False,
) -> None:
    cond = val1 is val2
    if not cond:
        txt = "'%s' is '%s'" % (val1, val2)
        _dfatal(txt, msg, *args, only_warning=only_warning)


def dassert_is_not(
    val1: Any,
    val2: Optional[Any],
    msg: Optional[str] = None,
    *args: Any,
    only_warning: bool = False,
) -> None:
    cond = val1 is not val2
    if not cond:
        txt = "'%s' is not '%s'" % (val1, val2)
        _dfatal(txt, msg, *args, only_warning=only_warning)


def dassert_type_is(
    val1: Any,
    val2: Any,
    msg: Optional[str] = None,
    *args: Any,
    only_warning: bool = False,
) -> None:
    # pylint: disable=unidiomatic-typecheck
    cond = type(val1) is val2
    if not cond:
        txt = "Type of '%s' is '%s' instead of '%s'" % (val1, type(val1), val2)
        _dfatal(txt, msg, *args, only_warning=only_warning)


# TODO(gp): This is redundant with dassert_isinstance(..., (str, float)).
def dassert_type_in(
    val1: Any,
    val2: Any,
    msg: Optional[str] = None,
    *args: Any,
    only_warning: bool = False,
) -> None:
    # pylint: disable=unidiomatic-typecheck
    cond = type(val1) in val2
    if not cond:
        txt = "Type of '%s' is '%s' not in '%s'" % (val1, type(val1), val2)
        _dfatal(txt, msg, *args, only_warning=only_warning)


def dassert_isinstance(
    val1: Any,
    val2: Union[type, Iterable[type]],
    msg: Optional[str] = None,
    *args: Any,
    only_warning: bool = False,
) -> None:
    cond = isinstance(val1, val2)  # type: ignore[arg-type]
    if not cond:
        txt = "Instance of '%s' is '%s' instead of '%s'" % (
            val1,
            type(val1),
            val2,
        )
        _dfatal(txt, msg, *args, only_warning=only_warning)


def dassert_issubclass(
    val1: Any,
    val2: Union[type, Iterable[type]],
    msg: Optional[str] = None,
    *args: Any,
    only_warning: bool = False,
) -> None:
    """
    Assert that an object `val1` is a subclass of `val2`.
    """
    cond = issubclass(val1.__class__, val2)  # type: ignore[arg-type]
    if not cond:
        txt = "Instance '%s' of class '%s' is not a subclass of '%s'" % (
            str(val1),
            val1.__class__.__name__,
            val2,
        )
        _dfatal(txt, msg, *args, only_warning=only_warning)


def dassert_callable(
    func: Any,
    msg: Optional[str] = None,
    *args: Any,
    only_warning: bool = False,
) -> None:
    """
    Assert that an object `val1` is callable.
    """
    cond = callable(func)
    if not cond:
        txt = "Obj '%s' of type '%s' is not callable" % (
            str(func),
            str(type(func)),
        )
        _dfatal(txt, msg, *args, only_warning=only_warning)


# Set related.


# TODO(gp): A more general solution is to have a function that traverses an obj
#  and creates a corresponding obj only with deterministic data structures (e.g.,
#  converting sets and dicts to sorted lists). Then we can print with `pprint`.
def _set_to_str(set_: Set[Any], thr: Optional[int] = 20) -> str:
    """
    Return a string with the ordered content of a set.

    This is useful when printing assertions that we want to be deterministic (e.g.,
    if we use it inside unit tests like:
    ```
    with self.assertRaises(AssertionError) as cm:
        ...
    act = str(cm.exception)
    exp = r
    """
    """
    self.assert_equal(act, exp, fuzzy_match=True)
    ```
    """
    list_ = sorted(list(set_))
    # If sets have less than `thr` elements print them as well, otherwise
    # print the beginning / end.
    if thr is not None and len(list_) > thr:
        txt = "%s [%s, ... %s]" % (len(list_), min(list_), max(list_))
    else:
        txt = str(list_)
    return txt


def dassert_set_eq(
    val1: Any,
    val2: Any,
    msg: Optional[str] = None,
    *args: Any,
    only_warning: bool = False,
) -> None:
    """
    Check that `val1` has the same elements as `val2`, raise otherwise.

    :param only_warning: issue a warning instead of aborting
    """
    val1 = set(val1)
    val2 = set(val2)
    # pylint: disable=superfluous-parens
    if not (val1 == val2):
        txt = []
        txt.append("val1 - val2=" + _set_to_str(val1.difference(val2)))
        txt.append("val2 - val1=" + _set_to_str(val2.difference(val1)))
        txt.append("val1=" + _set_to_str(val1))
        txt.append("set eq")
        txt.append("val2=" + _set_to_str(val2))
        _dfatal(txt, msg, *args, only_warning=only_warning)


# TODO(gp): -> dassert_issubset to match Python set function.
def dassert_is_subset(
    val1: Any,
    val2: Any,
    msg: Optional[str] = None,
    *args: Any,
    only_warning: bool = False,
) -> None:
    """
    Check that `val1` is a subset of `val2`, raise otherwise.
    """
    val1 = set(val1)
    val2 = set(val2)
    if not val1.issubset(val2):
        txt = []
        txt.append("val1=" + _set_to_str(val1))
        txt.append("issubset")
        txt.append("val2=" + _set_to_str(val2))
        txt.append("val1 - val2=" + _set_to_str(val1.difference(val2)))
        _dfatal(txt, msg, *args, only_warning=only_warning)


# TODO(gp): -> dassert_no_intersection to match other functions.
def dassert_not_intersection(
    val1: Any,
    val2: Any,
    msg: Optional[str] = None,
    *args: Any,
    only_warning: bool = False,
) -> None:
    """
    Check that `val1` has no intersection `val2`, raise otherwise.
    """
    val1 = set(val1)
    val2 = set(val2)
    if val1.intersection(val2):
        txt = []
        txt.append("val1=" + _set_to_str(val1))
        txt.append("has no intersection")
        txt.append("val2=" + _set_to_str(val2))
        txt.append("val1 - val2=" + _set_to_str(val1.difference(val2)))
        _dfatal(txt, msg, *args, only_warning=only_warning)


# Array related.


def dassert_no_duplicates(
    val1: Any, msg: Optional[str] = None, *args: Any, only_warning: bool = False
) -> None:
    cond = len(set(val1)) == len(val1)
    if not cond:
        # Count the occurrences of each element of the seq.
        v_to_num = [(v, val1.count(v)) for v in set(val1)]
        # Build list of elements with duplicates.
        dups = [v for v, n in v_to_num if n > 1]
        txt = []
        txt.append("val1=\n" + pprint.pformat(val1))
        txt.append("has duplicates")
        txt.append(",".join(map(str, dups)))
        _dfatal(txt, msg, *args, only_warning=only_warning)


def dassert_is_sorted(
    val1: Union[List, Tuple],
    sort_kwargs: Optional[Dict[Any, Any]] = None,
    msg: Optional[str] = None,
    *args: Any,
    only_warning: bool = False,
) -> None:
    # TODO(gp): Extend for pd.Series using the proper method.
    dassert_isinstance(val1, (list, tuple))
    sort_kwargs = {} if sort_kwargs is None else sort_kwargs
    sorted_val1 = sorted(val1, **sort_kwargs)
    cond = sorted_val1 == val1
    if not cond:
        txt = []
        txt.append("val1=\n" + pprint.pformat(val1))
        txt.append("is not sorted")
        txt.append("sorted(val1)=\n" + pprint.pformat(sorted_val1))
        _dfatal(txt, msg, *args, only_warning=only_warning)


def dassert_eq_all(
    val1: Any,
    val2: Any,
    msg: Optional[str] = None,
    *args: Any,
    only_warning: bool = False,
) -> None:
    val1 = list(val1)
    val2 = list(val2)
    cond = val1 == val2
    if not cond:
        # mask = val1 != val2
        txt = []
        txt.append("val1=%s\n%s" % (len(val1), val1))
        txt.append("val2=%s\n%s" % (len(val2), val2))
        # txt += "\ndiff=%s" % mask.sum()
        # txt += "\n%s" % val1[mask]
        # txt += "\n%s" % val2[mask]
        _dfatal(txt, msg, *args, only_warning=only_warning)


def _get_first_type(obj: Iterable, tag: str) -> Type:
    obj_types = set(type(v) for v in obj)
    dassert_eq(
        len(obj_types),
        1,
        "More than one type for elem of " "%s=%s",
        tag,
        map(str, obj_types),
    )
    return list(obj_types)[0]


def dassert_array_has_same_type_element(
    obj1: Any,
    obj2: Any,
    only_first_elem: bool,
    msg: Optional[str] = None,
    *args: Any,
    only_warning: bool = False,
) -> None:
    """
    Check that two objects iterables like arrays (e.g., pd.Index) have elements
    of the same type.

    :param only_first_elem: whether to check only the first element or all the
        elements of the iterable.
    """
    # Get the types to compare.
    if only_first_elem:
        obj1_first_type = type(obj1[0])
        obj2_first_type = type(obj2[0])
    else:
        obj1_first_type = _get_first_type(obj1, "obj1")
        obj2_first_type = _get_first_type(obj2, "obj2")
    #
    if obj1_first_type != obj2_first_type:
        txt = []
        num_elems = 5
        txt.append("obj1=\n%s" % obj1[:num_elems])
        txt.append("obj2=\n%s" % obj2[:num_elems])
        txt.append(
            "type(obj1)='%s' is different from "
            "type(obj2)='%s'" % (obj1_first_type, obj2_first_type)
        )
        _dfatal(txt, msg, *args, only_warning=only_warning)


def dassert_container_type(
    obj: Any,
    container_type: Optional[Any],
    elem_type: Optional[Any],
    msg: Optional[str] = None,
    *args: Any,
    only_warning: bool = False,
) -> None:
    """
    Assert `obj` is a certain type of container containing certain type of
    objects.

    E.g., `obj` is a list of strings.
    """
    # Add information about the obj.
    if not msg:
        msg = ""
    msg = msg.rstrip("\n") + "\nobj='%s'" % str(obj)
    # Check container.
    if container_type is not None:
        dassert_isinstance(
            obj, container_type, msg, *args, only_warning=only_warning
        )
    # Check the elements of the container.
    if elem_type is not None:
        for elem in obj:
            dassert_isinstance(
                elem, elem_type, msg, *args, only_warning=only_warning
            )


# TODO(gp): @all Replace calls to this with calls to `dassert_container_type()`.
def dassert_list_of_strings(
    list_: List[str],
    msg: Optional[str] = None,
    *args: Any,
    only_warning: bool = False,
) -> None:
    # TODO(gp): Allow iterable?
    dassert_isinstance(list_, list, msg, *args, only_warning=only_warning)
    for elem in list_:
        dassert_isinstance(elem, str, msg, *args, only_warning=only_warning)


# File related.


# TODO(*): Deprecate this and use only `dassert_{file,dir}_exists()`.
def dassert_exists(
    file_name: str,
    msg: Optional[str] = None,
    *args: Any,
    only_warning: bool = False,
) -> None:
    file_name = os.path.abspath(file_name)
    if not os.path.exists(file_name):
        txt = []
        txt.append("File '%s' doesn't exist" % file_name)
        _dfatal(txt, msg, *args, only_warning=only_warning)


def dassert_file_exists(
    file_name: str,
    msg: Optional[str] = None,
    *args: Any,
    only_warning: bool = False,
) -> None:
    """
    Assert unless `file_name` exists and it's a file and not a directory.
    """
    file_name = os.path.abspath(file_name)
    # `file_name` exists.
    exists = os.path.exists(file_name)
    if not exists:
        txt = f"File '{file_name}' doesn't exist"
        _dfatal(txt, msg, *args, only_warning=only_warning)
    # `file_name` is a file.
    is_file = os.path.isfile(file_name)
    if not is_file:
        txt = f"'{file_name}' is not a file"
        _dfatal(txt, msg, *args, only_warning=only_warning)


def dassert_dir_exists(
    dir_name: str,
    msg: Optional[str] = None,
    *args: Any,
    only_warning: bool = False,
) -> None:
    """
    Assert unless `dir_name` exists and it's a directory.
    """
    dir_name = os.path.abspath(dir_name)
    # `dir_name` exists.
    exists = os.path.exists(dir_name)
    if not exists:
        txt = f"Dir '{dir_name}' doesn't exist"
        _dfatal(txt, msg, *args, only_warning=only_warning)
    # `dir_name` is a directory.
    is_dir = os.path.isdir(dir_name)
    if not is_dir:
        txt = f"'{dir_name}' is not a dir"
        _dfatal(txt, msg, *args, only_warning=only_warning)


def dassert_not_exists(
    file_name: str,
    msg: Optional[str] = None,
    *args: Any,
    only_warning: bool = False,
) -> None:
    """
    Ensure that a file or a dir `file_name` doesn't exist, raise otherwise.

    Of course, we don't need to distinguish between
    `dassert_file_not_exists()` and `dassert_dir_not_exists()` because
    if something doesn't exist, we can't make a distinction of what it
    is.
    """
    file_name = os.path.abspath(file_name)
    # pylint: disable=superfluous-parens,unneeded-not
    if not (not os.path.exists(file_name)):
        txt = []
        txt.append("file='%s' already exists" % file_name)
        _dfatal(txt, msg, *args, only_warning=only_warning)


# TODO(gp): Does it work for a file ending in ".pkl.gz"? Add unit test.
def dassert_file_extension(
    file_name: str, extensions: Union[str, List[str]], only_warning: bool = False
) -> None:
    """
    Ensure that file has one of the given extensions.

    :param extensions: don't need to start with `.`, e.g., use `csv` instead of
        `.csv`
    """
    # Handle single extension case.
    if isinstance(extensions, str):
        extensions = [extensions]
    # Make sure extension starts with .
    extensions = [f".{e}" if not e.startswith(".") else e for e in extensions]
    # Check.
    act_ext = os.path.splitext(file_name)[-1].lower()
    dassert_in(
        act_ext,
        extensions,
        "Invalid extension '%s' for file '%s'",
        act_ext,
        file_name,
        only_warning=only_warning,
    )


# #############################################################################
# Logger.
# #############################################################################


# TODO(gp): Move this to helpers/hlogging.py and change all the callers.

# TODO(gp): maybe replace "force_verbose_format" and "force_print_format" with
#  a "mode" in ("auto", "verbose", "print")
def init_logger(
    verbosity: int = logging.INFO,
    use_exec_path: bool = False,
    log_filename: Optional[str] = None,
    force_verbose_format: bool = False,
    force_print_format: bool = False,
    force_white: bool = True,
    force_no_warning: bool = False,
    in_pytest: bool = False,
    report_memory_usage: bool = False,
    report_cpu_usage: bool = False,
) -> None:
    """
    Send stderr and stdout to logging (optionally teeing the logs to file).

    - Note that:
        - logging.DEBUG = 10
        - logging.INFO = 20

    :param verbosity: verbosity to use
    :param use_exec_path: use the name of the executable
    :param log_filename: log to that file
    :param force_verbose_format: use the verbose format for the logging
    :param force_print_format: use the print format for the logging
    :param force_white: use white color for printing. This can pollute the
        output of a script when redirected to file with echo characters
    :param in_pytest: True when we are running through pytest, so that we
        can overwrite the default logger from pytest
    :param report_memory_usage: turn on reporting memory usage
    :param report_cpu_usage: turn on reporting CPU usage
    """
    # Try to minimize dependencies.
    import helpers.hlogging as hloggin

    # TODO(gp): Print the stacktrace every time is called.
    if force_white:
        sys.stdout.write("\033[0m")
    if isinstance(verbosity, str):
        # pylint: disable=protected-access
        verbosity = logging._checkLevel(verbosity)
    # From https://stackoverflow.com/questions/14058453
    root_logger = logging.getLogger()
    # Set verbosity for all loggers.
    root_logger.setLevel(verbosity)
    # if False:
    #     eff_level = root_logger.getEffectiveLevel()
    #     print(
    #         "effective level= %s (%s)"
    #         % (eff_level, logging.getLevelName(eff_level))
    #     )
    # if False:
    #     # dassert_eq(root_logger.getEffectiveLevel(), verbosity)
    #     for handler in root_logger.handlers:
    #         handler.setLevel(verbosity)
    # Exit to avoid to replicate the same output multiple times.
    if not in_pytest and root_logger.handlers:
        print(WARNING + ": Logger already initialized: skipping")
        return
    #
    print(INFO + ": > cmd='%s'" % get_command_line())
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(verbosity)
    # Set the formatter.
    # formatter = hloggin.set_v1_formatter(
    formatter = hloggin.set_v2_formatter(
        ch,
        root_logger,
        force_no_warning,
        force_print_format,
        force_verbose_format,
        report_memory_usage,
        report_cpu_usage,
    )
    # Find name of the log file.
    if use_exec_path and log_filename is None:
        dassert_is(log_filename, None, msg="Can't specify conflicting filenames")
        # Use the name of the executable.
        import inspect

        frame = inspect.stack()[1]
        module = inspect.getmodule(frame[0])
        if not hasattr(module, __file__):
            filename = module.__file__  # type: ignore
        else:
            filename = "unknown_module"
        log_filename = os.path.realpath(filename) + ".log"
    # Handle teeing to a file.
    if log_filename:
        # Create dir if it doesn't exist.
        log_dirname = os.path.dirname(log_filename)
        if log_dirname != "" and not os.path.exists(log_dirname):
            os.mkdir(log_dirname)
        # Delete the file since we don't want to append.
        if os.path.exists(log_filename):
            os.unlink(log_filename)
        # Tee to file.
        file_handler = logging.FileHandler(log_filename)
        root_logger.addHandler(file_handler)
        file_handler.setFormatter(formatter)
        #
        print(INFO + ": Saving log to file '%s'" % log_filename)
    #
    _LOG.debug("Effective logging level=%s", _LOG.getEffectiveLevel())
    # Shut up chatty modules.
    hloggin.shutup_chatty_modules(verbose=False)
    #
    # test_logger()


def set_logger_verbosity(
    verbosity: int, module_name: Optional[str] = None
) -> None:
    """
    Change the verbosity of the logging after the initialization.

    Passing a module_name (e.g., matplotlib) one can change the logging of
    that specific module.

    E.g., set_logger_verbosity(logging.WARNING, "matplotlib")
    """
    logger = logging.getLogger(module_name)
    if module_name is None and not logger.handlers:
        assert 0, "ERROR: Logger not initialized"
    logger.setLevel(verbosity)
    eff_level = logger.getEffectiveLevel()
    print(
        "effective level= %s (%s)" % (eff_level, logging.getLevelName(eff_level))
    )
    dassert_eq(logger.getEffectiveLevel(), verbosity)


def get_logger_verbosity() -> int:
    root_logger = logging.getLogger()
    if not root_logger.handlers:
        assert 0, "ERROR: Logger not initialized"
    return root_logger.getEffectiveLevel()


# #############################################################################
# Command line.
# #############################################################################


# Sample at the beginning of time before we start fiddling with command line
# args.
_CMD_LINE = " ".join(arg for arg in sys.argv)
_EXEC_NAME = os.path.abspath(sys.argv[0])


def get_command_line() -> str:
    return _CMD_LINE


def get_exec_name() -> str:
    return _EXEC_NAME
