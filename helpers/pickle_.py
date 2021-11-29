"""
Pickle and JSON serialization/deserialization routines.

Import as:

import helpers.pickle_ as hpickle
"""

import gzip
import json
import logging
import marshal
import os
import pickle
import types
from typing import Any, Callable

import helpers.dbg as hdbg
import helpers.introspection as hintros
import helpers.io_ as hio
import helpers.timer as htimer

_LOG = logging.getLogger(__name__)


# #############################################################################
# pickle
# #############################################################################


def to_pickle(
    obj: Any,
    file_name: str,
    backend: str = "pickle",
    log_level: int = logging.DEBUG,
) -> None:
    """
    Pickle object `obj` into file `file_name`.

    :param file_name: the file_name is not changed, but it is checked for
        consistency with the backend (e.g., `pickle_gzip` needs a `.pkl.gz`
        extension)
    :param backend: pickle, dill, pickle_gzip
    """
    hdbg.dassert_type_is(file_name, str)
    hio.create_enclosing_dir(file_name, incremental=True)
    with htimer.TimedScope(logging.DEBUG, "Pickling to '%s'" % file_name) as ts:
        # We assume that the user always specifies a .pkl extension and then we
        # change the extension based on the backend.
        if backend in ("pickle", "dill"):
            hdbg.dassert_file_extension(file_name, "pkl")
            if backend == "pickle":
                with open(file_name, "wb") as fd:
                    pickler = pickle.Pickler(fd, pickle.HIGHEST_PROTOCOL)
                    pickler.fast = True
                    pickler.dump(obj)
            elif backend == "dill":
                import dill

                with open(file_name, "wb") as fd:
                    dill.dump(obj, fd)
            else:
                raise ValueError("Invalid backend='%s'" % backend)
        elif backend == "pickle_gzip":
            # TODO(gp): Use `dassert_file_extension` if possible.
            hdbg.dassert(
                file_name.endswith(".pkl.gz"),
                msg="Invalid file_name=%s" % file_name,
            )
            with gzip.open(file_name, "wb") as zfd:
                pickler = pickle.Pickler(zfd, pickle.HIGHEST_PROTOCOL)
                pickler.fast = True
                pickler.dump(obj)
        else:
            raise ValueError("Invalid backend='%s'" % backend)
    # Report time and size.
    file_size = hintros.format_size(os.path.getsize(file_name))
    _LOG.log(
        log_level,
        "Saved '%s' (size=%s, time=%.1fs)",
        file_name,
        file_size,
        ts.elapsed_time,
    )


def from_pickle(
    file_name: str,
    backend: str = "pickle",
    log_level: int = logging.DEBUG,
) -> Any:
    """
    Unpickle and return object stored in `file_name`.
    """
    hdbg.dassert_isinstance(file_name, str)
    with htimer.TimedScope(
        logging.DEBUG, "Unpickling from '%s'" % file_name
    ) as ts:
        # We assume that the user always specifies a .pkl extension and then we
        # change the extension based on the backend.
        if backend in ("pickle", "dill"):
            hdbg.dassert_file_extension(file_name, "pkl")
            if backend == "pickle":
                with open(file_name, "rb") as fd:
                    unpickler = pickle.Unpickler(fd)
                    obj = unpickler.load()
            elif backend == "dill":
                import dill

                with open(file_name, "rb") as fd:
                    obj = dill.load(fd)
            else:
                raise ValueError("Invalid backend='%s'" % backend)
        elif backend == "pickle_gzip":
            # TODO(gp): Use `dassert_file_extension` if possible.
            hdbg.dassert(
                file_name.endswith(".pkl.gz"),
                msg="Invalid file_name=%s" % file_name,
            )
            with gzip.open(file_name, "rb") as zfd:
                unpickler = pickle.Unpickler(zfd)
                obj = unpickler.load()
        else:
            raise ValueError("Invalid backend='%s'" % backend)
    # Report time and size.
    file_size = hintros.format_size(os.path.getsize(file_name))
    _LOG.log(
        log_level,
        "Read '%s' (size=%s, time=%.1fs)",
        file_name,
        file_size,
        ts.elapsed_time,
    )
    return obj


# #############################################################################


# TODO(gp): -> to_pickle_function
def pickle_function(func: Callable) -> str:
    """
    Pickle a function into bytecode stored into a string.

    - return: string
    """
    hdbg.dassert_callable(func)
    code_as_bytes = marshal.dumps(func.__code__)
    return code_as_bytes.decode()


# TODO(gp): -> from_pickle_function
def unpickle_function(code_as_str: str, func_name: str) -> Callable:
    """
    Unpickle a function saved into string <code_as_str>. The function is
    injected in the global namespace as <func_name>.

    - return: function
    """
    hdbg.dassert_isinstance(code_as_str, str)
    code = marshal.loads(code_as_str.encode())
    func = types.FunctionType(code, globals(), name=func_name)
    return func


# #############################################################################
# JSON
# #############################################################################

# TODO(gp): Maybe move helpers/hjson.py?


# TODO(gp): Switch file_name and obj to be consistent with the pickle functions.
def to_json(file_name: str, obj: object) -> None:
    hdbg.dassert_file_extension(file_name, "json")
    with open(file_name, "w") as outfile:
        json.dump(obj, outfile)


def from_json(file_name: str) -> object:
    hdbg.dassert_exists(file_name)
    hdbg.dassert_file_extension(file_name, "json")
    obj = json.loads(hio.from_file(file_name))
    return obj
