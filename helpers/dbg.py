import logging
import os
import pprint
import sys

#try:
#    import pandas as pd
#    _HAS_PANDAS = True
#except ImportError:
#    _HAS_PANDAS = False

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
    ret = "\n" + _frame(message, '#', 80)
    if assertion_type is None:
        assertion_type = AssertionError
    raise assertion_type(ret)


# #############################################################################
# dassert.
# #############################################################################


# TODO(gp): Use None as default everywhere.
def __to_msg(msg):
    return ("(" + msg + ") ") if msg != "" else ""


def dassert(cond, msg=""):
    if not cond:
        dfatal("* Failed assertion %s %s*" % (cond, __to_msg(msg)))


def dassert_eq(val1, val2, msg=""):
    if not val1 == val2:
        dfatal("* Failed assertion:\n'%s'\n==\n'%s'\n%s*" % (val1, val2,
                                                             __to_msg(msg)))


def dassert_ne(val1, val2, msg=""):
    # pylint: disable=C0325
    if not (val1 != val2):
        dfatal("* Failed assertion:\n'%s'\n!=\n'%s'\n%s*" % (val1, val2,
                                                             __to_msg(msg)))


def dassert_lt(val1, val2, msg=""):
    # pylint: disable=C0325
    if not (val1 < val2):
        dfatal("* Failed assertion: %s < %s %s*" % (val1, val2, __to_msg(msg)))


def dassert_lte(val1, val2, msg=""):
    # pylint: disable=C0325
    if not (val1 <= val2):
        dfatal("* Failed assertion: %s <= %s %s*" % (val1, val2, __to_msg(msg)))


def dassert_lgt(lower_bound, x, upper_bound, lower_bound_closed,
                upper_bound_closed):
    if lower_bound_closed:
        dassert_lte(lower_bound, x)
    else:
        dassert_lt(lower_bound, x)
    if upper_bound_closed:
        dassert_lte(x, upper_bound)
    else:
        dassert_lt(x, upper_bound)


def dassert_in(value, valid_values, msg=""):
    # pylint: disable=C0325
    if not (value in valid_values):
        dfatal("* Failed assertion: '%s' in '%s' %s*" % (value, valid_values,
                                                         __to_msg(msg)))


def dassert_not_in(value, valid_values, msg=""):
    if value in valid_values:
        dfatal("* Failed assertion: '%s' not in '%s' %s*" %
               (value, valid_values, __to_msg(msg)))


def dassert_is(val1, val2, msg=""):
    # pylint: disable=C0325
    if not (val1 is val2):
        dfatal(
            "* Failed assertion: '%s' is '%s' %s" % (val1, val2, __to_msg(msg)))


def dassert_type_is(val1, val2, msg=""):
    # pylint: disable=C0325, C0123
    if not (type(val1) is val2):
        dfatal("* Failed assertion: type of '%s' is '%s' instead of '%s' %s" %
               (val1, type(val1), val2, __to_msg(msg)))


def dassert_type_in(val1, val2, msg=""):
    # pylint: disable=C0325, C0123
    if not (type(val1) in val2):
        dfatal("* Failed assertion: type of '%s' is '%s' not in '%s' %s" %
               (val1, type(val1), val2, __to_msg(msg)))


def dassert_isinstance(val1, val2, msg=""):
    if not isinstance(val1, val2):
        dfatal(
            "* Failed assertion: instance '%s' is of type '%s' instead of "
            "expected type '%s' %s" % (val1, type(val1), val2, __to_msg(msg)))


def dassert_is_not(val1, val2, msg=""):
    # pylint: disable=C0325
    if not (val1 is not val2):
        dfatal("* Failed assertion: '%s' is not '%s' %s" % (val1, val2,
                                                            __to_msg(msg)))


def dassert_imply(val1, val2, msg=""):
    # pylint: disable=C0325
    if not (not val1 or val2):
        dfatal(
            "* Failed assertion: '%s' => '%s' %s" % (val1, val2, __to_msg(msg)))


def dassert_set_eq(val1, val2, msg=""):
    val1 = set(val1)
    val2 = set(val2)
    # pylint: disable=C0325
    if not (val1 == val2):
        msg_ = ("* Failed assertion:\n" + "val1 - val2=" + str(
            val1.difference(val2)) + "\n" + "val2 - val1=" + str(
                val2.difference(val1)) + "\n")
        thr = 20
        if max(len(val1), len(val2)) < thr:
            msg_ += ("val1=" + pprint.pformat(val1) + "\n" + "  set eq\n" +
                     "val2=" + pprint.pformat(val2) + "\n")
        msg += " " + __to_msg(msg)
        dfatal(msg_)


def dassert_is_subset(val1, val2, msg="", verbose=False):
    val1 = set(val1)
    val2 = set(val2)
    if not val1.issubset(val2):
        msg_ = "* Failed assertion:\n"
        if verbose:
            msg_ += ("val1=" + pprint.pformat(val1) + "\n" + "  issubset \n" +
                     "val2=" + pprint.pformat(val2) + "\n")
        msg_ += "val1 - val2=" + str(
            val1.difference(val2)) + "\n" + __to_msg(msg)
        dfatal(msg_)


def dassert_not_intersection(val1, val2, msg=""):
    val1 = set(val1)
    val2 = set(val2)
    if val1.intersection(val2):
        msg_ = (
            "* Failed assertion:\n" + "val1=" + pprint.pformat(val1) + "\n" +
            "  issubset \n" + "val2=" + pprint.pformat(val2) + "\n" +
            "val1 - val2=" + str(val1.difference(val2)) + "\n" + __to_msg(msg))
        dfatal(msg_)


def dassert_no_duplicates(val1, msg=""):
    # pylint: disable=C0325
    if not (len(set(val1)) == len(val1)):
        # Count the occurrences of each element of the seq.
        v_to_num = [(v, val1.count(v)) for v in set(val1)]
        # Build list of elems with duplicates.
        res = [v for v, n in v_to_num if n > 1]
        msg_ = (
            "* Failed assertion:\n" + "val1=" + pprint.pformat(val1) + "\n" +
            "has duplicates=" + ",".join(map(str, res)) + "\n" + __to_msg(msg))
        dfatal(msg_)


def dassert_exists(file_name, msg=""):
    file_name = os.path.abspath(file_name)
    if not os.path.exists(file_name):
        # yapf: disable
        msg_ = ("* Failed assertion:\n" +
                "file='%s' doesn't exist" % file_name +
                " " + __to_msg(msg))
        # yapf: enable
        dfatal(msg_)


def dassert_not_exists(file_name, msg=""):
    file_name = os.path.abspath(file_name)
    # pylint: disable=C0325
    if not (not os.path.exists(file_name)):
        # yapf: disable
        msg_ = ("* Failed assertion:\n" +
                "file='%s' already exists" % file_name +
                " " + __to_msg(msg))
        # yapf: enable
        dfatal(msg_)


def dassert_dir_exists(dir_name, msg=""):
    dir_name = os.path.abspath(dir_name)
    # pylint: disable=C0325
    if not (os.path.exists(dir_name) and not os.path.isdir(dir_name)):
        # yapf: disable
        msg_ = ("* Failed assertion:\n" +
                "dir_name='%s' already exists" % dir_name +
                " " + __to_msg(msg))
        # yapf: enable
        dfatal(msg_)


## TODO(gp): -> dassert_timestamp
#if _HAS_PANDAS:
#
#    def assert_timestamp(ts, msg=None):
#        """
#        Check that input is a pandas.Timestamp or datetime.datetime and that
#        it is set to 'US/Eastern' timezone.
#        """
#        ts_types = set([pd.Timestamp, datetime.datetime])
#        dassert_type_in(ts, ts_types)
#        if msg is None:
#            msg = 'Timezone must be US/Eastern!'
#        dassert_ne(ts.tzinfo, None, msg)
#        dassert_eq(ts.tzinfo.zone, 'US/Eastern', msg)

# #############################################################################
# Logger.
# #############################################################################


# From https://stackoverflow.com/questions/15411967/how-can-i-check-if-code-is-executed-in-the-ipython-notebook
def is_running_in_ipynb():
    try:
        cfg = get_ipython().config
        res = cfg['IPKernelApp']['parent_appname'] == 'ipython-notebook'
    except NameError:
        res = False
    return res


# TODO(gp): Add an option to log as if it was a print for notebook use.
def init_logger(verb=logging.INFO, use_exec_path=False, log_filename=None):
    """
    - Send both stderr and stdout to logging.
    - Optionally tee the logs also to file.

    :param verb: verbosity to use
    :param use_exec_path: use the name of the executable
    :param log_filename: log to that file
    :return:
    """
    # yapf: disable
    # pylint: disable=C0301
    # From https://stackoverflow.com/questions/14058453/making-python-loggers-output-all-messages-to-stdout-in-addition-to-log
    # yapf: enable
    root_logger = logging.getLogger()
    root_logger.setLevel(verb)
    #print("effective level=", root_logger.getEffectiveLevel())
    if len(root_logger.handlers) > 0:
        print("WARNING: Logger already initialized: skipping")
        return
    #
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(verb)
    if is_running_in_ipynb():
        print("WARNING: Running in Jupyter")
        #_LOG_FORMAT = "%(asctime)-15s: %(levelname)s %(message)s"
        # TODO(gp): Print at much 15-20 chars of a function so that things are aligned
        _LOG_FORMAT = "%(levelname)-5s: %(funcName)-15s: %(message)s"
    else:
        _LOG_FORMAT = "%(asctime)-5s: %(levelname)s: %(funcName)s: %(message)s"
    formatter = logging.Formatter(_LOG_FORMAT, datefmt='%Y-%m-%d %I:%M:%S %p')
    ch.setFormatter(formatter)
    root_logger.addHandler(ch)
    # Find name of the log file.
    if use_exec_path and log_filename is None:
        dassert_is(log_filename, None, msg="Can't specify conflicting filenames")
        # Use the name of the executable.
        import inspect
        frame = inspect.stack()[1]
        module = inspect.getmodule(frame[0])
        filename = module.__file__
        log_filename = os.path.realpath(filename) + ".log"
    # Handle teeing to a file.
    if log_filename:
        print("Saving log to file '%s'" % log_filename)
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
        _LOG = logging.getLogger(__name__)
        _LOG.info("Saving log to '%s'", log_filename)
    #
    #test_logger()


def set_logger_verb(verb):
    """
    Used to change the verbosity of the logging after the initialization.
    """
    root_logger = logging.getLogger()
    if len(root_logger.handlers) == 0:
        assert 0, "ERROR: Logger not initialized"
    root_logger.setLevel(verb)


# TODO(gp): Remove this.
def init_logger2(verb=logging.INFO):
    # flake8: noqa
    # pylint: disable=C0301
    # From https://stackoverflow.com/questions/14058453/making-python-loggers-output-all-messages-to-stdout-in-addition-to-log
    root = logging.getLogger()
    root.setLevel(verb)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(verb)
    log_format = "%(asctime)-15s %(funcName)-20s: %(levelname)-5s %(message)s"
    formatter = logging.Formatter(log_format, datefmt='%Y-%m-%d %I:%M:%S %p')
    ch.setFormatter(formatter)
    root.addHandler(ch)


def test_logger():
    print("# Testing logger ...")
    _log = logging.getLogger(__name__)
    print("effective level=", _log.getEffectiveLevel())
    #
    print("logging.DEBUG=", logging.DEBUG)
    _log.debug("*working*")
    #
    print("logging.INFO=", logging.INFO)
    _log.info("*working*")
    #
    print("logging.WARNING=", logging.WARNING)
    _log.warning("*working*")
    #
    print("logging.CRITICAL=", logging.CRITICAL)
    _log.critical("*working*")
