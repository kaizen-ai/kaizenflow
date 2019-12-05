import gzip
import json
import logging
import marshal
import os
import pickle
import types

import helpers.dbg as dbg
import helpers.io_ as io_
import helpers.timer as timer

_LOG = logging.getLogger(__name__)


def _replace_extension(file_name, ext):
    dbg.dassert(not ext.startswith("."), msg="ext='%s'" % ext)
    return "%s.%s" % (os.path.splitext(file_name)[0], ext)


def to_pickle(obj, file_name, backend="pickle", log_level=logging.DEBUG):
    """
    Pickle object <obj> into file <file_name>.
    """
    dbg.dassert_type_is(file_name, str)
    dtmr = timer.dtimer_start(log_level, "Pickling to '%s'" % file_name)
    io_.create_enclosing_dir(file_name, incremental=True)
    # We assume that the user always specifies a .pkl extension and then we
    # change the extension based on the backend.
    if backend in ("pickle", "dill"):
        dbg.dassert(
            file_name.endswith(".pkl"), msg="Invalid file_name=%s" % file_name
        )
        if backend == "pickle":
            with open(file_name, "wb") as fd:
                pickler = pickle.Pickler(fd, pickle.HIGHEST_PROTOCOL)
                pickler.fast = True
                pickler.dump(obj)
        elif backend == "dill":
            import dill

            with open(file_name, "wb") as fd:
                pickler = dill.dump(obj, fd)
    elif backend == "pickle_gzip":
        dbg.dassert(
            file_name.endswith(".pkl.gz"), msg="Invalid file_name=%s" % file_name
        )
        with gzip.open(file_name, "wb") as fd:
            pickler = pickle.Pickler(fd, pickle.HIGHEST_PROTOCOL)
            pickler.fast = True
            pickler.dump(obj)
    else:
        raise ValueError("Invalid backend='%s'" % backend)
    _, elapsed_time = timer.dtimer_stop(dtmr)
    size_mb = os.path.getsize(file_name) / (1024.0 ** 2)
    _LOG.info(
        "Saved '%s' (size=%.2f Mb, time=%.1fs)", file_name, size_mb, elapsed_time
    )


def from_pickle(file_name, backend="pickle", log_level=logging.DEBUG):
    """
    Unpickle and return object stored in <file_name>.
    """
    dbg.dassert_type_is(file_name, str)
    dtmr = timer.dtimer_start(log_level, "Unpickling from '%s'" % file_name)
    # We assume that the user always specifies a .pkl extension and then we
    # change the extension based on the backend.
    if backend in ("pickle", "dill"):
        dbg.dassert(
            file_name.endswith(".pkl"), msg="Invalid file_name=%s" % file_name
        )
        if backend == "pickle":
            with open(file_name, "rb") as fd:
                unpickler = pickle.Unpickler(fd)
                obj = unpickler.load()
        elif backend == "dill":
            import dill

            with open(file_name, "rb") as fd:
                obj = dill.load(fd)
    elif backend == "pickle_gzip":
        dbg.dassert(
            file_name.endswith(".pkl.gz"), msg="Invalid file_name=%s" % file_name
        )
        with gzip.open(file_name, "rb") as fd:
            unpickler = pickle.Unpickler(fd)
            obj = unpickler.load()
    else:
        raise ValueError("Invalid backend='%s'" % backend)
    _, elapsed_time = timer.dtimer_stop(dtmr)
    size_mb = os.path.getsize(file_name) / (1024.0 ** 2)
    _LOG.info(
        "Read '%s' (size=%.2f Mb, time=%.1fs)", file_name, size_mb, elapsed_time
    )
    return obj


def pickle_function(func):
    """
    Pickle a function into bytecode stored into a string.
    - return: string
    """
    dbg.dassert(callable(func))
    code_as_str = marshal.dumps(func.__code__)
    return code_as_str


def unpickle_function(code_as_str, func_name):
    """
    Unpickle a function saved into string <code_as_str>. The function is injected
    in the global namespace as <func_name>.
    - return: function
    """
    dbg.dassert_type_is(code_as_str, str)
    code = marshal.loads(code_as_str)
    func = types.FunctionType(code, globals(), name=func_name)
    return func


# #############################################################################
# json
# #############################################################################


def to_json(file_name, obj):
    with open(file_name, "w") as outfile:
        json.dump(obj, outfile)


def from_json(file_name):
    dbg.dassert_exists(file_name)
    obj = json.loads(io_.from_file(file_name))
    return obj
