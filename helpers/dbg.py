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

# TODO(gp): Would be nice to have a way to disable the assertions in certain
#  builds, or at least know how much time is spent in the assertions.


# TODO(gp): "msg" should not be the str and be just the format and then pass all
# the arguments like in log, to avoid computing the string every time.

# TODO(gp): Use None as default everywhere and propagate this idiom everywhere.
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
        #res = "(" + res + ") "
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
        txt = "instance of '%s' is '%s' instead of '%s'" % (val1, type(val1),
                                                           val2)
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
        #mask = val1 != val2
        txt = []
        txt.append("val1=%s\n%s" % (len(val1), val1))
        txt.append("val2=%s\n%s" % (len(val2), val2))
        #txt += "\ndiff=%s" % mask.sum()
        #txt += "\n%s" % val1[mask]
        #txt += "\n%s" % val2[mask]
        _dfatal(txt, msg, *args)


# TODO(*): -> _file_exists
def dassert_exists(file_name, msg=None, *args):
    file_name = os.path.abspath(file_name)
    if not os.path.exists(file_name):
        txt = []
        txt.append("file='%s' doesn't exist" % file_name)
        _dfatal(txt, msg, *args)


# TODO(*): -> _file_not_exists
def dassert_not_exists(file_name, msg=None, *args):
    file_name = os.path.abspath(file_name)
    # pylint: disable=C0325,C0113
    if not (not os.path.exists(file_name)):
        txt = []
        txt.append("file='%s' already exists" % file_name)
        _dfatal(txt, msg, *args)


def dassert_dir_exists(dir_name, msg=None, *args):
    dir_name = os.path.abspath(dir_name)
    # pylint: disable=C0325
    if not (os.path.exists(dir_name) and not os.path.isdir(dir_name)):
        txt = []
        txt.append("dir='%s' already exists" % dir_name)
        _dfatal(txt, msg, *args)


def dassert_monotonic_index(obj):
    dassert(obj.index.is_monotonic_increasing)
    dassert(obj.index.is_unique)


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


# From https://stackoverflow.com/questions/15411967
def is_running_in_ipynb():
    try:
        _ = get_ipython().config
        #res = cfg['IPKernelApp']['parent_appname'] == 'ipython-notebook'
        res = True
    except NameError:
        res = False
    return res


def reset_logger():
    from importlib import reload
    print("Resetting logger...")
    logging.shutdown()
    reload(logging)


def init_logger(verb=logging.INFO,
                use_exec_path=False,
                log_filename=None,
                force_verbose_format=False):
    """
    - Send both stderr and stdout to logging.
    - Optionally tee the logs also to file.

    :param verb: verbosity to use
    :param use_exec_path: use the name of the executable
    :param log_filename: log to that file
    :param force_verbose_format: use the verbose format used by code even for
        notebook
    """
    # From https://stackoverflow.com/questions/14058453
    root_logger = logging.getLogger()
    root_logger.setLevel(verb)
    #print("effective level=", root_logger.getEffectiveLevel())
    if root_logger.handlers:
        print("WARNING: Logger already initialized: skipping")
        return
    #
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(verb)
    if not force_verbose_format and is_running_in_ipynb():
        print("WARNING: Running in Jupyter")
        # Make logging look like a normal print().
        log_format = "%(message)s"
        datefmt = ''
    else:
        # TODO(gp): Print at much 15-20 chars of a function so that things are aligned
        #log_format = "%(levelname)-5s: %(funcName)-15s: %(message)s"
        log_format = "%(asctime)-5s: %(levelname)s: %(funcName)s: %(message)s"
        datefmt = '%Y-%m-%d %I:%M:%S %p'
    formatter = logging.Formatter(log_format, datefmt=datefmt)
    ch.setFormatter(formatter)
    root_logger.addHandler(ch)
    # Find name of the log file.
    if use_exec_path and log_filename is None:
        dassert_is(
            log_filename, None, msg="Can't specify conflicting filenames")
        # Use the name of the executable.
        import inspect
        frame = inspect.stack()[1]
        module = inspect.getmodule(frame[0])
        filename = module.__file__
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
    #test_logger()


def set_logger_verb(verb):
    """
    Used to change the verbosity of the logging after the initialization.
    """
    root_logger = logging.getLogger()
    if not root_logger.handlers:
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
