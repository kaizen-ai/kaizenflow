import pickle
import json
import logging
import marshal
import os
import types

import helpers.debug as dbg

_log = logging.getLogger(__name__)


def _replace_extension(file_name, ext):
    dbg.dassert(not ext.startswith("."), msg="ext='%s'" % ext)
    return "%s.%s" % (os.path.splitext(file_name)[0], ext)


def pickle(file_name, obj, backend="pickle", verb=10):
    """
    Pickle object <obj> into file <file_name>.
    """
    dbg.dassert_type_is(file_name, str)
    dtmr = utils.timer.dtimer_start(verb, "Pickling to '%s'" % file_name)
    utils.jio.create_enclosing_dir(file_name, incremental=True)
    # We assume that the user always specifies a .pkl extension and then we
    # change the extension based on the backend.
    dbg.dassert(
        file_name.endswith(".pkl"), msg="Invalid file_name=%s" % file_name)
    if backend == "pickle":
        with open(file_name, 'wb') as fd:
            pickler = pickle.Pickler(fd, pickle.HIGHEST_PROTOCOL)
            pickler.fast = True
            pickler.dump(obj)
    elif backend == "dill":
        import dill
        with open(file_name, 'wb') as fd:
            pickler = dill.dump(obj, fd)
    else:
        raise ValueError("Invalid backend='%s'" % backend)
    _, elapsedTime = utils.timer.dtimer_stop(dtmr)
    fileSizeInKb = os.path.getsize(file_name) / 1024.0
    # We can't use jnumpy.Div() since we want to avoid a dependency between utils
    # files.
    transferRate = ("%s.3f" % (fileSizeInKb / elapsedTime)
                    if elapsedTime != 0.0 else "n/a")
    log.debug(
        ("Pickling: fileSize=%.1f Kb, time=%.3f -> " + "transferRate=%s Kb/s") %
        (fileSizeInKb, elapsedTime, transferRate))


def unpickle(file_name, backend="pickle", verb=10):
    """
    Unpickle and return object stored in <file_name>.
    """
    dbg.dassert_type_is(file_name, str)
    dtmr = utils.timer.dtimer_start(verb, "Unpickling from '%s'" % file_name)
    # We assume that the user always specifies a .pkl extension and then we
    # change the extension based on the backend.
    dbg.dassert(
        file_name.endswith(".pkl"), msg="Invalid file_name=%s" % file_name)
    if backend == "pickle":
        with open(file_name, 'rb') as fd:
            unpickler = pickle.Unpickler(fd)
            obj = unpickler.load()
    elif backend == "dill":
        import dill
        with open(file_name, 'rb') as fd:
            obj = dill.load(fd)
    else:
        raise ValueError("Invalid backend='%s'" % backend)
    _, elapsedTime = utils.timer.dtimer_stop(dtmr)
    fileSizeInKb = os.path.getsize(file_name) / 1024.0
    transferRate = ("%.3f" % (fileSizeInKb / elapsedTime)
                    if elapsedTime != 0.0 else "n/a")
    log.debug(
        ("Unpickling: fileSize=%.1f Kb, time=%.3f -> " + "transferRate=%s Kb/s")
        % (fileSizeInKb, elapsedTime, transferRate))
    return obj


def pickle_function(func):
    """
    Pickle a function into bytecode stored into a string.
    - return: string
    """
    dbg.dassert(callable(func))
    codeString = marshal.dumps(func.__code__)
    return codeString


def unpickle_function(codeString, functionName):
    """
    Unpickle a function saved into string <codeString>. The function is injected
    in the global namespace as <functionName>.
    - return: function
    """
    dbg.dassert_type_is(codeString, str)
    code = marshal.loads(codeString)
    func = types.FunctionType(code, globals(), name=functionName)
    return func


def to_json(file_name, obj):
    with open(file_name, 'w') as outfile:
        json.dump(obj, outfile)


def from_json(file_name):
    dbg.dassert_file_exists(file_name)
    obj = json.loads(utils.jio.from_file(file_name, split=False))
    return obj
