"""
Import as:

import helpers.dbg as dbg
"""

import copy
import datetime
import logging
import os
import pprint
import sys
from typing import Any, Dict, Iterable, List, Optional, Tuple, Type, Union

# import helpers.versioning as hversi
# hversi.check_version()

# This module should not depend on anything else than Python standard modules.

_LOG = logging.getLogger(__name__)

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


# INVARIANT:
# - `dassert_COND()` checks that COND is true, and raises if COND is False
# - For this reason the condition inside the `dassert` is typically in the form
#   `if not (...):`, even this might annoy the linter or look weird


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
    txt: Union[str, Iterable[str]], msg: Optional[str], *args: Any
) -> None:
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
    dfatal(dfatal_txt)


def dassert(cond: Any, msg: Optional[str] = None, *args: Any) -> None:
    # Handle the somehow frequent case of using `dassert` instead of another
    # one, e.g., `dassert(y, list)`
    if msg is not None:
        assert isinstance(
            msg, str
        ), f"You passed '{msg}' or type '{type(msg)}' instead of str"
    if not cond:
        txt = "cond=%s" % cond
        _dfatal(txt, msg, *args)


def dassert_eq(
    val1: Any, val2: Any, msg: Optional[str] = None, *args: Any
) -> None:
    cond = val1 == val2
    if not cond:
        txt = "'%s'\n==\n'%s'" % (val1, val2)
        _dfatal(txt, msg, *args)


def dassert_ne(
    val1: Any, val2: Any, msg: Optional[str] = None, *args: Any
) -> None:
    cond = val1 != val2
    if not cond:
        txt = "'%s'\n!=\n'%s'" % (val1, val2)
        _dfatal(txt, msg, *args)


def dassert_imply(
    val1: Any, val2: Any, msg: Optional[str] = None, *args: Any
) -> None:
    cond = not val1 or val2
    if not cond:
        txt = "'%s' implies '%s'" % (val1, val2)
        _dfatal(txt, msg, *args)


# Comparison related.


def dassert_lt(
    val1: Any, val2: Any, msg: Optional[str] = None, *args: Any
) -> None:
    cond = val1 < val2
    if not cond:
        txt = "%s < %s" % (val1, val2)
        _dfatal(txt, msg, *args)


def dassert_lte(
    val1: Any, val2: Any, msg: Optional[str] = None, *args: Any
) -> None:
    cond = val1 <= val2
    if not cond:
        txt = "%s <= %s" % (val1, val2)
        _dfatal(txt, msg, *args)


def dassert_lgt(
    lower_bound: float,
    x: float,
    upper_bound: float,
    lower_bound_closed: bool,
    upper_bound_closed: bool,
    msg: Optional[str] = None,
    *args: Any,
) -> None:
    """
    Assert that `lower_bound <= x <= upper_bound`.

    :param lower_bound_closed, upper_bound_closed: control the
        open-ness/close-ness of the interval extremes.
    """
    # `lower_bound <= or < x`.
    if lower_bound_closed:
        dassert_lte(lower_bound, x, msg, *args)
    else:
        dassert_lt(lower_bound, x, msg, *args)
    # `x <= or < upper_bound`.
    if upper_bound_closed:
        dassert_lte(x, upper_bound, msg, *args)
    else:
        dassert_lt(x, upper_bound, msg, *args)


def dassert_is_proportion(
    x: float, msg: Optional[str] = None, *args: Any
) -> None:
    """
    Assert that `0 <= x <= 1`.
    """
    lower_bound_closed = True
    upper_bound_closed = True
    dassert_lgt(0, x, 1, lower_bound_closed, upper_bound_closed, msg, *args)


# Membership.


def dassert_in(
    value: Any, valid_values: Any, msg: Optional[str] = None, *args: Any
) -> None:
    cond = value in valid_values
    if not cond:
        txt = "'%s' in '%s'" % (value, valid_values)
        _dfatal(txt, msg, *args)


def dassert_not_in(
    value: Any, valid_values: Iterable[Any], msg: Optional[str] = None, *args: Any
) -> None:
    cond = value not in valid_values
    if not cond:
        txt = "'%s' not in '%s'" % (value, valid_values)
        _dfatal(txt, msg, *args)


# Type related.


def dassert_is(
    val1: Optional[str],
    val2: Optional[Any],
    msg: Optional[str] = None,
    *args: Any,
) -> None:
    cond = val1 is val2
    if not cond:
        txt = "'%s' is '%s'" % (val1, val2)
        _dfatal(txt, msg, *args)


def dassert_is_not(
    val1: Any, val2: Optional[Any], msg: Optional[str] = None, *args: Any
) -> None:
    cond = val1 is not val2
    if not cond:
        txt = "'%s' is not '%s'" % (val1, val2)
        _dfatal(txt, msg, *args)


def dassert_type_is(
    val1: Any, val2: Any, msg: Optional[str] = None, *args: Any
) -> None:
    # pylint: disable=unidiomatic-typecheck
    cond = type(val1) is val2
    if not cond:
        txt = "type of '%s' is '%s' instead of '%s'" % (val1, type(val1), val2)
        _dfatal(txt, msg, *args)


# TODO(gp): This is redundant with dassert_isinstance(..., (str, float)).
def dassert_type_in(
    val1: Any, val2: Any, msg: Optional[str] = None, *args: Any
) -> None:
    # pylint: disable=unidiomatic-typecheck
    cond = type(val1) in val2
    if not cond:
        txt = "type of '%s' is '%s' not in '%s'" % (val1, type(val1), val2)
        _dfatal(txt, msg, *args)


def dassert_isinstance(
    val1: Any,
    val2: Union[type, Iterable[type]],
    msg: Optional[str] = None,
    *args: Any,
) -> None:
    cond = isinstance(val1, val2)  # type: ignore[arg-type]
    if not cond:
        txt = "instance of '%s' is '%s' instead of '%s'" % (
            val1,
            type(val1),
            val2,
        )
        _dfatal(txt, msg, *args)


# Set related.


def dassert_set_eq(
    val1: Any, val2: Any, msg: Optional[str] = None, *args: Any
) -> None:
    val1 = set(val1)
    val2 = set(val2)
    # pylint: disable=superfluous-parens
    if not (val1 == val2):
        txt = []
        txt.append("val1 - val2=" + str(val1.difference(val2)))
        txt.append("val2 - val1=" + str(val2.difference(val1)))
        thr = 20
        if max(len(val1), len(val2)) < thr:
            txt.append("val1=" + pprint.pformat(val1))
            txt.append("set eq")
            txt.append("val2=" + pprint.pformat(val2))
        _dfatal(txt, msg, *args)


# TODO(gp): -> dassert_issubset to match Python set function.
def dassert_is_subset(
    val1: Any, val2: Any, msg: Optional[str] = None, *args: Any
) -> None:
    """
    Check that val1 is a subset of val2, raise otherwise.
    """
    val1 = set(val1)
    val2 = set(val2)
    if not val1.issubset(val2):
        txt = []
        txt.append("val1=" + pprint.pformat(val1))
        txt.append("issubset")
        txt.append("val2=" + pprint.pformat(val2))
        txt.append("val1 - val2=" + str(val1.difference(val2)))
        _dfatal(txt, msg, *args)


def dassert_not_intersection(
    val1: Any, val2: Any, msg: Optional[str] = None, *args: Any
) -> None:
    """
    Check that val1 has no intersection val2, raise otherwise.
    """
    val1 = set(val1)
    val2 = set(val2)
    if val1.intersection(val2):
        txt = []
        txt.append("val1=" + pprint.pformat(val1))
        txt.append("has no intersection")
        txt.append("val2=" + pprint.pformat(val2))
        txt.append("val1 - val2=" + str(val1.difference(val2)))
        _dfatal(txt, msg, *args)


# Array related.


def dassert_no_duplicates(
    val1: Any, msg: Optional[str] = None, *args: Any
) -> None:
    cond = len(set(val1)) == len(val1)
    if not cond:
        # Count the occurrences of each element of the seq.
        v_to_num = [(v, val1.count(v)) for v in set(val1)]
        # Build list of elems with duplicates.
        dups = [v for v, n in v_to_num if n > 1]
        txt = []
        txt.append("val1=\n" + pprint.pformat(val1))
        txt.append("has duplicates")
        txt.append(",".join(map(str, dups)))
        _dfatal(txt, msg, *args)


def dassert_is_sorted(
    val1: Union[List, Tuple],
    sort_kwargs: Optional[Dict[Any, Any]] = None,
    msg: Optional[str] = None,
    *args: Any,
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
        _dfatal(txt, msg, *args)


def dassert_eq_all(
    val1: Any, val2: Any, msg: Optional[str] = None, *args: Any
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
        _dfatal(txt, msg, *args)


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
        _dfatal(txt, msg, *args)


def dassert_container_type(
    obj: Any,
    container_type: Optional[Any],
    elem_type: Optional[Any],
    msg: Optional[str] = None,
    *args: Any,
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
        dassert_isinstance(obj, container_type, msg, *args)
    # Check the elements of the container.
    if elem_type is not None:
        for elem in obj:
            dassert_isinstance(elem, elem_type, msg, *args)


# TODO(gp): Replace calls to this with calls to `dassert_container_type()`.
def dassert_list_of_strings(
    list_: List[str], msg: Optional[str] = None, *args: Any
) -> None:
    # TODO(gp): Allow iterable?
    dassert_isinstance(list_, list, msg, *args)
    for elem in list_:
        dassert_isinstance(elem, str, msg, *args)


# File related.


# TODO(*): Deprecate this and use only `dassert_{file,dir}_exists()`.
def dassert_exists(file_name: str, msg: Optional[str] = None, *args: Any) -> None:
    file_name = os.path.abspath(file_name)
    if not os.path.exists(file_name):
        txt = []
        txt.append("File '%s' doesn't exist" % file_name)
        _dfatal(txt, msg, *args)


def dassert_file_exists(
    file_name: str, msg: Optional[str] = None, *args: Any
) -> None:
    """
    Assert unless `file_name` exists and it's a file and not a directory.
    """
    file_name = os.path.abspath(file_name)
    # `file_name` exists.
    exists = os.path.exists(file_name)
    if not exists:
        txt = f"File '{file_name}' doesn't exist"
        _dfatal(txt, msg, *args)
    # `file_name` is a file.
    is_file = os.path.isfile(file_name)
    if not is_file:
        txt = f"'{file_name}' is not a file"
        _dfatal(txt, msg, *args)


def dassert_dir_exists(
    dir_name: str, msg: Optional[str] = None, *args: Any
) -> None:
    """
    Assert unless `dir_name` exists and it's a directory.
    """
    dir_name = os.path.abspath(dir_name)
    # `dir_name` exists.
    exists = os.path.exists(dir_name)
    if not exists:
        txt = f"Dir '{dir_name}' doesn't exist"
        _dfatal(txt, msg, *args)
    # `dir_name` is a directory.
    is_dir = os.path.isdir(dir_name)
    if not is_dir:
        txt = f"'{dir_name}' is not a dir"
        _dfatal(txt, msg, *args)


def dassert_not_exists(
    file_name: str, msg: Optional[str] = None, *args: Any
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
        _dfatal(txt, msg, *args)


def dassert_file_extension(
    file_name: str, extensions: Union[str, List[str]]
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
    )


# Pandas related.

# TODO(gp): Consider moving these to `dbg_pandas.py` and avoid the implicit
#  dependency from pandas.


def dassert_index_is_datetime(
    df: "pd.DataFrame", msg: Optional[str] = None, *args: Any
) -> None:
    """
    Ensure that the dataframe has an index containing datetimes.
    """
    import pandas as pd

    # TODO(gp): Add support also for series.
    dassert_isinstance(df, pd.DataFrame, msg, *args)
    dassert_isinstance(df.index, pd.DatetimeIndex, msg, *args)


def dassert_strictly_increasing_index(
    obj: Union["pd.Index", "pd.DataFrame", "pd.Series"],
    msg: Optional[str] = None,
    *args: Any,
) -> None:
    """
    Ensure that the dataframe has a strictly increasing index.
    """
    import pandas as pd

    if isinstance(obj, pd.Index):
        index = obj
    else:
        index = obj.index
    # TODO(gp): Understand why mypy reports:
    #   error: "dassert" gets multiple values for keyword argument "msg"
    dassert(index.is_monotonic_increasing, msg=msg, *args)  # type: ignore
    dassert(index.is_unique, msg=msg, *args)  # type: ignore


# TODO(gp): Factor out common code related to extracting the index from several
#  pandas data structures.
# TODO(gp): Not sure it's used or useful?
def dassert_monotonic_index(
    obj: Union["pd.Index", "pd.DataFrame", "pd.Series"],
    msg: Optional[str] = None,
    *args: Any,
) -> None:
    """
    Ensure that the dataframe has a strictly increasing or decreasing index.
    """
    # For some reason importing pandas is slow and we don't want to pay this
    # start up cost unless we have to.
    import pandas as pd

    if isinstance(obj, pd.Index):
        index = obj
    else:
        index = obj.index
    # TODO(gp): Understand why mypy reports:
    #   error: "dassert" gets multiple values for keyword argument "msg"
    cond = index.is_monotonic_increasing or index.is_monotonic_decreasing
    dassert(cond, msg=msg, *args)  # type: ignore
    dassert(index.is_unique, msg=msg, *args)  # type: ignore


# #############################################################################
# Logger.
# #############################################################################

# TODO(gp): Separate this to helpers/log.py

# Copied from helpers/system_interaction.py to avoid circular imports.
def _is_running_in_ipynb() -> bool:
    try:
        _ = get_ipython().config  # type: ignore
        res = True
    except NameError:
        res = False
    return res


def reset_logger() -> None:
    import importlib

    print("Resetting logger...")
    logging.shutdown()
    importlib.reload(logging)


# TODO(gp): Make these generate from MAPPING below.
INFO = "\033[36mINFO\033[0m"
WARNING = "\033[33mWARNING\033[0m"
ERROR = "\033[31mERROR\033[0m"


# From https://stackoverflow.com/questions/32402502
class _LocalTimeZoneFormatter:
    """
    Override logging.Formatter to use an aware datetime object.
    """

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        try:
            # TODO(gp): Automatically detect the time zone. It might be complicated in
            #  Docker.
            from dateutil import tz

            # self._tzinfo = pytz.timezone('America/New_York')
            self._tzinfo = tz.gettz("America/New_York")
        except ModuleNotFoundError as e:
            print("Can't import dateutil: using UTC\n%s" % str(e))
            self._tzinfo = None

    def converter(self, timestamp: float) -> datetime.datetime:
        # To make the linter happy and respecting the signature of the
        # superclass method.
        _ = self
        # timestamp=1622423570.0147252
        dt = datetime.datetime.utcfromtimestamp(timestamp)
        # Convert it to an aware datetime object in UTC time.
        dt = dt.replace(tzinfo=datetime.timezone.utc)
        if self._tzinfo is not None:
            # Convert it to desired timezone.
            dt = dt.astimezone(self._tzinfo)
        return dt

    def formatTime(
        self, record: logging.LogRecord, datefmt: Optional[str] = None
    ) -> str:
        dt = self.converter(record.created)
        if datefmt:
            s = dt.strftime(datefmt)
        else:
            try:
                s = dt.isoformat(timespec="milliseconds")
            except TypeError:
                s = dt.isoformat()
        return s


# [mypy] error: Definition of "converter" in base class
# "_LocalTimeZoneFormatter" is incompatible with definition in base class
# "Formatter"
class _ColoredFormatter(  # type: ignore[misc]
    _LocalTimeZoneFormatter, logging.Formatter
):
    """
    Logging formatter using colors for different levels.
    """

    MAPPING = {
        # White: 37.
        # Blu.
        "DEBUG": (34, "DEBUG"),
        # Cyan.
        "INFO": (36, "INFO "),
        # Yellow.
        "WARNING": (33, "WARN "),
        # Red.
        "ERROR": (31, "ERROR"),
        # White on red background.
        "CRITICAL": (41, "CRTCL"),
    }

    def __init__(self, log_format: str, date_format: str):
        super().__init__(log_format, date_format)

    def format(self, record: logging.LogRecord) -> str:
        colored_record = copy.copy(record)
        # `levelname` is the internal name and can't be changed to `level_name`
        # as per our conventions.
        levelname = colored_record.levelname
        # Use white as default.
        prefix = "\033["
        suffix = "\033[0m"
        assert levelname in self.MAPPING, "Can't find info '%s'"
        color_code, tag = self.MAPPING[levelname]
        # Align the level name.
        colored_levelname = "{0}{1}m{2}{3}".format(
            prefix, color_code, tag, suffix
        )
        colored_record.levelname = colored_levelname
        return logging.Formatter.format(self, colored_record)


# From https://stackoverflow.com/questions/10848342
# and https://docs.python.org/3/howto/logging-cookbook.html#filters-contextual
class ResourceUsageFilter(logging.Filter):
    """
    Add fields to the logger about memory and CPU use.
    """

    def __init__(self, report_cpu_usage: bool):
        super().__init__()
        import psutil

        self._process = psutil.Process()
        self._report_cpu_usage = report_cpu_usage
        if self._report_cpu_usage:
            # Start sampling the CPU usage.
            self._process.cpu_percent(interval=1.0)

    def filter(self, record: logging.LogRecord) -> bool:
        """
        Override `logging.Filter()`, adding several fields to the logger.
        """
        p = self._process
        # Report memory usage.
        rss_in_GB = p.memory_info().rss / (1024 ** 3)
        vms_in_GB = p.memory_info().vms / (1024 ** 3)
        mem_pct = p.memory_percent()
        resource_use = "rss=%.1fGB vms=%.1fGB mem_pct=%.0f%%" % (
            rss_in_GB,
            vms_in_GB,
            mem_pct,
        )
        # Report CPU usage.
        if self._report_cpu_usage:
            # CPU usage since the previous call.
            cpu_use = p.cpu_percent(interval=None)
            resource_use += " cpu=%.0f%%" % cpu_use
        record.resource_use = resource_use  # type: ignore
        return True


# Copied from `helpers/system_interaction.py` to avoid circular dependencies.
def get_user_name() -> str:
    import getpass

    res = getpass.getuser()
    return res


# TODO(gp): Replace `force_print_format` and `force_verbose_format` with `mode`.
def _get_logging_format(
    force_print_format: bool,
    force_verbose_format: bool,
    force_no_warning: bool,
    report_resource_usage: bool,
    date_format_mode: str = "date_time",
) -> Tuple[str, str]:
    """
    Compute the logging format depending whether running on notebook or in a
    shell.

    The logging format can be:
    - print: looks like a `print` statement

    :param force_print_form: force to use the non-verbose format
    :param force_verbose_format: force to use the verbose format
    :param force_no_warning:
    """
    if _is_running_in_ipynb() and not force_no_warning:
        print(WARNING + ": Running in Jupyter")
    verbose_format = not _is_running_in_ipynb()
    #
    dassert(
        not (force_verbose_format and force_print_format),
        "Can't use both force_verbose_format=%s and force_print_format=%s",
        force_verbose_format,
        force_print_format,
    )
    if force_verbose_format:
        verbose_format = True
    if force_print_format:
        verbose_format = False
        #
    if verbose_format:
        # TODO(gp): We would like to have filename:name:funcName:lineno all
        #  justified on 15 chars.
        #  See https://docs.python.org/3/howto/logging-cookbook.html#use-of
        #  -alternative-formatting-styles
        #  Something like:
        #   {{asctime}-5s {{filename}{name}{funcname}{linedo}d}-15s {message}
        #
        # %(pathname)s Full pathname of the source file where the logging call was
        #   issued (if available).
        # %(filename)s Filename portion of pathname.
        # %(module)s Module (name portion of filename).
        if True:
            log_format = (
                # 04-28_08:08 INFO :
                "%(asctime)-5s %(levelname)-5s"
            )
            if report_resource_usage:
                # rss=0.3GB vms=2.0GB mem_pct=2% cpu=91%
                log_format += " [%(resource_use)-40s]"
            log_format += (
                # lib_tasks _delete_branches
                " %(module)-20s: %(funcName)-30s:"
                # 142: ...
                " %(lineno)-4d:"
                " %(message)s"
            )
        else:
            # Super verbose: to help with debugging print more info without trimming.
            log_format = (
                # 04-28_08:08 INFO :
                "%(asctime)-5s %(levelname)-5s"
                # .../src/lem1/amp/helpers/system_interaction.py
                # _system       :
                " %(pathname)s %(funcName)-20s "
                # 199: ...
                " %(lineno)d:"
                " %(message)s"
            )
        if date_format_mode == "time":
            date_fmt = "%H:%M"
        elif date_format_mode == "date_time":
            date_fmt = "%m-%d_%H:%M"
        elif date_format_mode == "date_timestamp":
            date_fmt = "%Y-%m-%d %I:%M:%S %p"
        else:
            raise ValueError("Invalid date_format_mode='%s'" % date_format_mode)
    else:
        # Make logging look like a normal print().
        # TODO(gp): We want to still prefix with WARNING and ERROR.
        log_format = "%(message)s"
        date_fmt = ""
    return date_fmt, log_format


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
    :param force_write: use white color for printing. This can pollute the
        output of a script when redirected to file with echo characters
    :param in_pytest: True when we are running through pytest, so that we
        can overwrite the default logger from pytest
    """
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
    # Turn on reporting memory and CPU usage.
    report_resource_usage = report_cpu_usage = False
    #
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(verbosity)
    # Decide whether to use verbose or print format.
    date_fmt, log_format = _get_logging_format(
        force_print_format,
        force_verbose_format,
        force_no_warning,
        report_resource_usage,
    )
    # Use normal formatter.
    # formatter = logging.Formatter(log_format, datefmt=date_fmt)
    # Use formatter with colors.
    formatter = _ColoredFormatter(log_format, date_fmt)
    ch.setFormatter(formatter)
    root_logger.addHandler(ch)
    # Report resource usage.
    if report_resource_usage:
        # Get root logger.
        log = logging.getLogger("")
        # Create filter.
        f = ResourceUsageFilter(report_cpu_usage)
        # The ugly part:adding filter to handler.
        log.handlers[0].addFilter(f)
    #
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
    shutup_chatty_modules(verbose=False)
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


def get_all_loggers() -> List:
    """
    Return list of all registered loggers.
    """
    logger_dict = logging.root.manager.loggerDict  # type: ignore
    loggers = [logging.getLogger(name) for name in logger_dict]
    return loggers


def get_matching_loggers(
    module_names: Union[str, Iterable[str]], verbose: bool
) -> List:
    """
    Find loggers that match a name or a name in a set.
    """
    if isinstance(module_names, str):
        module_names = [module_names]
    loggers = get_all_loggers()
    if verbose:
        print("loggers=\n", "\n".join(map(str, loggers)))
    #
    sel_loggers = []
    for module_name in module_names:
        if verbose:
            print("module_name=%s" % module_name)
        # TODO(gp): We should have a regex.
        # str(logger) looks like `<Logger tornado.application (DEBUG)>`
        sel_loggers_tmp = [
            logger
            for logger in loggers
            if str(logger).startswith("<Logger " + module_name)
            # logger for logger in loggers if module_name in str(logger)
        ]
        # print(sel_loggers_tmp)
        sel_loggers.extend(sel_loggers_tmp)
    if verbose:
        print("sel_loggers=%s" % sel_loggers)
    return sel_loggers


def shutup_chatty_modules(
    verbosity: int = logging.CRITICAL, verbose: bool = False
) -> None:
    """
    Reduce the verbosity for external modules that are very chatty.

    :param verbosity: level of verbosity used for chatty modules: the higher the
        better
    :param verbose: print extra information
    """
    module_names = [
        "aiobotocore",
        "asyncio",
        "boto",
        "boto3",
        "botocore",
        "fsspec",
        "hooks",
        # "ib_insync",
        "invoke",
        "matplotlib",
        "nose",
        "s3fs",
        "s3transfer",
        "urllib3",
    ]
    loggers = get_matching_loggers(module_names, verbose)
    loggers = sorted(loggers, key=lambda logger: logger.name)
    for logger in loggers:
        logger.setLevel(verbosity)
    if len(loggers) > 0:
        _LOG.debug(
            "Shut up %d modules: %s",
            len(loggers),
            ", ".join([logger.name for logger in loggers]),
        )
        # if _LOG.getEffectiveLevel() < logging.DEBUG:
        #    print(WARNING +
        #       " Shutting up %d modules: %s"
        #       % (len(loggers), ", ".join([logger.name for logger in loggers]))
        #    )


def test_logger() -> None:
    print("# Testing logger ...")
    _log = logging.getLogger(__name__)
    print("effective level=", _log.getEffectiveLevel())
    #
    _log.debug("DEBUG=%s", logging.DEBUG)
    #
    _log.info("INFO=%s", logging.INFO)
    #
    _log.warning("WARNING=%s", logging.WARNING)
    #
    _log.critical("CRITICAL=%s", logging.CRITICAL)


# #############################################################################


# Sample at the beginning of time before we start fiddling with command line
# args.
_CMD_LINE = " ".join(arg for arg in sys.argv)
_EXEC_NAME = os.path.abspath(sys.argv[0])


def get_command_line() -> str:
    return _CMD_LINE


def get_exec_name() -> str:
    return _EXEC_NAME
