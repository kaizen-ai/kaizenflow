import copy
import logging
import os
import pprint
import sys

_LOG = logging.getLogger(__name__)

# #############################################################################
# dfatal.
# #############################################################################

# Copied from printing.py to avoid cyclical dependencies.


def _line(chars="#", num_cols=80):
    line_ = chars * num_cols + "\n"
    return line_


def _frame(x, chars="#", num_cols=80):
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


def dfatal(message, assertion_type=None):
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


def _to_msg(msg, *args):
    """
    Format the error message with the params.
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


def _dfatal(cond, msg, *args):
    """
    Package the error to raise.
    """
    txt = "* Failed assertion *\n"
    if isinstance(cond, list):
        txt += "\n".join(cond)
    else:
        txt += cond
    msg = _to_msg(msg, *args)
    if msg:
        if not txt.endswith("\n"):
            txt += "\n"
        txt += msg
    dfatal(txt)


def dassert(cond, msg=None, *args):
    if not cond:
        txt = "cond=%s" % cond
        _dfatal(txt, msg, *args)


def dassert_eq(val1, val2, msg=None, *args):
    if not val1 == val2:
        txt = "'%s'\n==\n'%s'" % (val1, val2)
        _dfatal(txt, msg, *args)


def dassert_ne(val1, val2, msg=None, *args):
    # pylint: disable=C0325
    if not (val1 != val2):
        txt = "'%s'\n!=\n'%s'" % (val1, val2)
        _dfatal(txt, msg, *args)


def dassert_lt(val1, val2, msg=None, *args):
    # pylint: disable=C0325
    if not (val1 < val2):
        txt = "'%s' < '%s'" % (val1, val2)
        _dfatal(txt, msg, *args)


def dassert_lte(val1, val2, msg=None, *args):
    # pylint: disable=C0325
    if not (val1 <= val2):
        txt = "'%s' <= '%s'" % (val1, val2)
        _dfatal(txt, msg, *args)


def dassert_lgt(
    lower_bound, x, upper_bound, lower_bound_closed, upper_bound_closed
):
    if lower_bound_closed:
        dassert_lte(lower_bound, x)
    else:
        dassert_lt(lower_bound, x)
    if upper_bound_closed:
        dassert_lte(x, upper_bound)
    else:
        dassert_lt(x, upper_bound)


def dassert_in(value, valid_values, msg=None, *args):
    # pylint: disable=C0325
    if not (value in valid_values):
        txt = "'%s' in '%s'" % (value, valid_values)
        _dfatal(txt, msg, *args)


def dassert_not_in(value, valid_values, msg=None, *args):
    if value in valid_values:
        txt = "'%s' not in '%s'" % (value, valid_values)
        _dfatal(txt, msg, *args)


def dassert_is(val1, val2, msg=None, *args):
    # pylint: disable=C0325
    if not (val1 is val2):
        txt = "'%s' is '%s'" % (val1, val2)
        _dfatal(txt, msg, *args)


def dassert_is_not(val1, val2, msg=None, *args):
    # pylint: disable=C0325
    if not (val1 is not val2):
        txt = "'%s' is not '%s'" % (val1, val2)
        _dfatal(txt, msg, *args)


def dassert_type_is(val1, val2, msg=None, *args):
    # pylint: disable=C0325, C0123
    if not (type(val1) is val2):
        txt = "type of '%s' is '%s' instead of '%s'" % (val1, type(val1), val2)
        _dfatal(txt, msg, *args)


def dassert_type_in(val1, val2, msg=None, *args):
    # pylint: disable=C0325, C0123
    if not (type(val1) in val2):
        txt = "type of '%s' is '%s' not in '%s'" % (val1, type(val1), val2)
        _dfatal(txt, msg, *args)


def dassert_isinstance(val1, val2, msg=None, *args):
    if not isinstance(val1, val2):
        txt = "instance of '%s' is '%s' instead of '%s'" % (
            val1,
            type(val1),
            val2,
        )
        _dfatal(txt, msg, *args)


def dassert_imply(val1, val2, msg=None, *args):
    # pylint: disable=C0325
    if not (not val1 or val2):
        txt = "'%s' implies '%s'" % (val1, val2)
        _dfatal(txt, msg, *args)


def dassert_set_eq(val1, val2, msg=None, *args):
    val1 = set(val1)
    val2 = set(val2)
    # pylint: disable=C0325
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


# TODO(gp): -> dassert_issubset
def dassert_is_subset(val1, val2, msg=None, *args):
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


def dassert_not_intersection(val1, val2, msg=None, *args):
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


def dassert_no_duplicates(val1, msg=None, *args):
    # pylint: disable=C0325
    if not (len(set(val1)) == len(val1)):
        # Count the occurrences of each element of the seq.
        v_to_num = [(v, val1.count(v)) for v in set(val1)]
        # Build list of elems with duplicates.
        dups = [v for v, n in v_to_num if n > 1]
        txt = []
        txt.append("val1=" + pprint.pformat(val1))
        txt.append("has duplicates")
        txt.append(",".join(map(str, dups)))
        _dfatal(txt, msg, *args)


def dassert_eq_all(val1, val2, msg=None, *args):
    val1 = list(val1)
    val2 = list(val2)
    is_equal = val1 == val2
    if not is_equal:
        # mask = val1 != val2
        txt = []
        txt.append("val1=%s\n%s" % (len(val1), val1))
        txt.append("val2=%s\n%s" % (len(val2), val2))
        # txt += "\ndiff=%s" % mask.sum()
        # txt += "\n%s" % val1[mask]
        # txt += "\n%s" % val2[mask]
        _dfatal(txt, msg, *args)


# TODO(*): -> _file_exists
def dassert_exists(file_name, msg=None, *args):
    file_name = os.path.abspath(file_name)
    if not os.path.exists(file_name):
        txt = []
        txt.append("file='%s' doesn't exist" % file_name)
        _dfatal(txt, msg, *args)


# TODO(*): -> _file_not_exist
def dassert_not_exists(file_name, msg=None, *args):
    file_name = os.path.abspath(file_name)
    # pylint: disable=C0325,C0113
    if not (not os.path.exists(file_name)):
        txt = []
        txt.append("file='%s' already exists" % file_name)
        _dfatal(txt, msg, *args)


# TODO(*): -> dassert_dir_not_exist
def dassert_dir_exists(dir_name, msg=None, *args):
    dir_name = os.path.abspath(dir_name)
    # pylint: disable=C0325
    # TODO(gp): Not sure it's correct.
    # if not (os.path.isdir(dir_name) and os.path.exists(dir_name)):
    if not (os.path.exists(dir_name) and not os.path.isdir(dir_name)):
        txt = []
        txt.append("dir='%s' already exists" % dir_name)
        _dfatal(txt, msg, *args)


def dassert_monotonic_index(obj, msg=None, *args):
    # For some reason importing pandas is slow and we don't want to pay this
    # start up cost unless we have to.
    import pandas as pd

    if isinstance(obj, pd.Index):
        index = obj
    else:
        index = obj.index
    dassert(index.is_monotonic_increasing, msg=msg, *args)
    dassert(index.is_unique, msg=msg, *args)


def dassert_array_has_same_type_element(
    obj1, obj2, only_first_elem, msg=None, *args
):
    """
    Check that two objects iterables like arrays (e.g., pd.Index) have
    elements of the same type.

    :param only_first_elem: whether to check only the first element or all the
        elements of the iterable.
    """
    # Get the types to compare.
    if only_first_elem:
        obj1_first_type = type(obj1[0])
        obj2_first_type = type(obj2[0])
    else:

        def _get_first_type(obj, tag):
            obj_types = set(type(v) for v in obj)
            dassert_eq(
                len(obj_types),
                1,
                "More than one type for elem of " "%s=%s",
                tag,
                map(str, obj_types),
            )
            return list(obj_types)[0]

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


# #############################################################################
# Logger.
# #############################################################################


# From https://stackoverflow.com/questions/15411967
def is_running_in_ipynb():
    try:
        _ = get_ipython().config
        # res = cfg['IPKernelApp']['parent_appname'] == 'ipython-notebook'
        res = True
    except NameError:
        res = False
    return res


def reset_logger():
    from importlib import reload

    print("Resetting logger...")
    logging.shutdown()
    reload(logging)


class _ColoredFormatter(logging.Formatter):

    MAPPING = {
        # White.
        "DEBUG": 37,
        # Cyan.
        "INFO": 36,
        # Yellow.
        "WARNING": 33,
        # Red.
        "ERROR": 31,
        # White on red background.
        "CRITICAL": 41,
    }

    PREFIX = "\033["
    SUFFIX = "\033[0m"

    def __init__(self, log_format, date_format):
        logging.Formatter.__init__(self, log_format, date_format)

    def format(self, record):
        colored_record = copy.copy(record)
        levelname = colored_record.levelname
        # Use white as default.
        seq = self.MAPPING.get(levelname, 37)
        # Align the level name.
        levelname = "%-5s" % levelname
        colored_levelname = "{0}{1}m{2}{3}".format(
            self.PREFIX, seq, levelname, self.SUFFIX
        )
        colored_record.levelname = colored_levelname
        return logging.Formatter.format(self, colored_record)


def _get_logging_format(force_print_format, force_verbose_format):
    if is_running_in_ipynb():
        print("WARNING: Running in Jupyter")
    verbose_format = not is_running_in_ipynb()
    dassert(
        not (force_verbose_format and force_print_format),
        ("Can't use both force_verbose_format=%s and " "force_print_format=%s")
        % (force_verbose_format, force_print_format),
    )
    if force_verbose_format:
        verbose_format = True
    if force_print_format:
        verbose_format = False
    if verbose_format:
        # TODO(gp): We would like to have filename.name.funcName:lineno all
        # justified on the 15.
        # See https://docs.python.org/3/howto/logging-cookbook.html#use-of-alternative-formatting-styles
        # Something like:
        #   {{asctime}-5s {{filename}{name}{funcname}{linedo}d}-15s {message}
        #
        # log_format = "%(asctime)-5s %(levelname)-5s: %(funcName)-15s: %(message)s"
        log_format = (
            "%(asctime)-5s %(levelname)-5s: "
            "%(funcName)-15s:%(lineno)-4d: "
            "%(message)s"
            # "[%(name)s][%(levelname)s]  %(message)s (%(filename)s:%(lineno)d)")
        )
        # date_fmt = "%Y-%m-%d %I:%M:%S %p"
        date_fmt = "%m-%d_%H:%M"
    else:
        # Make logging look like a normal print().
        # TODO(gp): We want to still prefix with WARNING and ERROR.
        log_format = "%(message)s"
        date_fmt = ""
    return date_fmt, log_format


# TODO(gp): maybe replace "force_verbose_format" and "force_print_format" with
# a "mode" in ("auto", "verbose", "print")
def init_logger(
    verb=logging.INFO,
    use_exec_path=False,
    log_filename=None,
    force_verbose_format=False,
    force_print_format=False,
):
    """
    - Send both stderr and stdout to logging.
    - Optionally tee the logs also to file.

    :param verb: verbosity to use
    :param use_exec_path: use the name of the executable
    :param log_filename: log to that file
    :param force_verbose_format: use the verbose format for the logging in any
        case, even for notebook
    :param force_print_format: use the print format for the logging in any case
    """
    sys.stdout.write("\033[0m")
    if isinstance(verb, str):
        # pylint: disable=W0212
        verb = logging._checkLevel(verb)
    # From https://stackoverflow.com/questions/14058453
    root_logger = logging.getLogger()
    # Set verbosity for all loggers.
    root_logger.setLevel(verb)
    # if False:
    #     eff_level = root_logger.getEffectiveLevel()
    #     print(
    #         "effective level= %s (%s)"
    #         % (eff_level, logging.getLevelName(eff_level))
    #     )
    # if False:
    #     # dassert_eq(root_logger.getEffectiveLevel(), verb)
    #     for handler in root_logger.handlers:
    #         handler.setLevel(verb)
    # Exit to avoid to replicate the same output multiple times.
    if root_logger.handlers:
        print("WARNING: Logger already initialized: skipping")
        return
    #
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(verb)
    # Decide whether to use verbose or print format.
    date_fmt, log_format = _get_logging_format(
        force_print_format, force_verbose_format
    )
    # Use normal formatter.
    # formatter = logging.Formatter(log_format, datefmt=date_fmt)
    # Use formatter with colors.
    formatter = _ColoredFormatter(log_format, date_fmt)
    ch.setFormatter(formatter)
    root_logger.addHandler(ch)
    #
    # Find name of the log file.
    if use_exec_path and log_filename is None:
        dassert_is(log_filename, None, msg="Can't specify conflicting filenames")
        # Use the name of the executable.
        import inspect

        frame = inspect.stack()[1]
        module = inspect.getmodule(frame[0])
        if not hasattr(module, __file__):
            filename = module.__file__
        else:
            filename = "unknown_module"
        log_filename = os.path.realpath(filename) + ".log"
    # Handle teeing to a file.
    if log_filename:
        # Create dir if it doesn't exist.
        log_dirname = os.path.dirname(log_filename)
        if not os.path.exists(log_dirname):
            os.mkdir(log_dirname)
        # Delete the file since we don't want to append.
        if os.path.exists(log_filename):
            os.unlink(log_filename)
        # Tee to file.
        file_handler = logging.FileHandler(log_filename)
        root_logger.addHandler(file_handler)
        file_handler.setFormatter(formatter)
        #
        print("Saving log to file '%s'" % log_filename)
    #
    # test_logger()


def set_logger_verb(verb, module_name=None):
    """
    Used to change the verbosity of the logging after the initialization.

    Passing a module_name (e.g., matplotlib) one can change the logging of
    that specific module.
    """
    logger = logging.getLogger(module_name)
    if module_name is None and not logger.handlers:
        assert 0, "ERROR: Logger not initialized"
    logger.setLevel(verb)
    eff_level = logger.getEffectiveLevel()
    print(
        "effective level= %s (%s)" % (eff_level, logging.getLevelName(eff_level))
    )
    dassert_eq(logger.getEffectiveLevel(), verb)


def get_logger_verb():
    root_logger = logging.getLogger()
    if not root_logger.handlers:
        assert 0, "ERROR: Logger not initialized"
    return root_logger.getEffectiveLevel()


def get_all_loggers():
    loggers = [
        logging.getLogger(name) for name in logging.root.manager.loggerDict
    ]
    return loggers


def get_matching_loggers(module_name):
    loggers = get_all_loggers()
    sel_loggers = [logger for logger in loggers if module_name in str(logger)]
    return sel_loggers


# TODO(gp): Remove this.
def init_logger2(verb=logging.INFO):
    # From https://stackoverflow.com/questions/14058453
    root = logging.getLogger()
    root.setLevel(verb)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(verb)
    log_format = "%(asctime)-15s %(funcName)-20s: %(levelname)-5s %(message)s"
    formatter = logging.Formatter(log_format, datefmt="%Y-%m-%d %I:%M:%S %p")
    ch.setFormatter(formatter)
    root.addHandler(ch)


def test_logger():
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


def get_command_line():
    return _CMD_LINE
