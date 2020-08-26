"""Import as:

import helpers.introspection as intr
"""

import collections.abc as abc
import inspect
import sys
from typing import Any, List

import helpers.dbg as dbg


def is_iterable(obj):
    """Return whether obj can be iterated upon or not.

    Note that a string is iterable in python, but typically we refer to
    iterables as lists, tuples, so we exclude it.
    """
    # From https://stackoverflow.com/questions/1952464
    return not isinstance(obj, str) and isinstance(obj, abc.Iterable)


def get_function_name(count=0):
    """Return the name of the function calling this function, i.e., the name of
    the function calling `get_function_name()`."""
    ptr = inspect.currentframe()
    # count=0 corresponds to the calling function, so we need to add an extra
    # step walking the call stack.
    count += 1
    for _ in range(count):
        dbg.dassert_is_not(ptr, None)
        ptr = ptr.f_back
    func_name = ptr.f_code.co_name
    return func_name


# From https://github.com/bosswissam/pysize
def get_size(obj, seen=None):
    """Recursively find size of an object `obj` in bytes."""
    size = sys.getsizeof(obj)
    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0
    # Important mark as seen *before* entering recursion to gracefully handle
    # self-referential objects.
    seen.add(obj_id)
    if hasattr(obj, "__dict__"):
        for cls in obj.__class__.__mro__:
            if "__dict__" in cls.__dict__:
                d = cls.__dict__["__dict__"]
                if inspect.isgetsetdescriptor(d) or inspect.ismemberdescriptor(d):
                    size += get_size(obj.__dict__, seen)
                break
    if isinstance(obj, dict):
        size += sum((get_size(v, seen) for v in obj.values()))
        size += sum((get_size(k, seen) for k in obj.keys()))
    elif hasattr(obj, "__iter__") and not isinstance(
        obj, (str, bytes, bytearray)
    ):
        size += sum((get_size(i, seen) for i in obj))
    if hasattr(obj, "__slots__"):  # can have __slots__ with __dict__
        size += sum(
            get_size(getattr(obj, s), seen)
            for s in obj.__slots__
            if hasattr(obj, s)
        )
    return size


def get_methods(obj: Any, access: str = "all") -> List[str]:
    """Return list of names corresponding to class methods of an object `obj`.

    :param obj: class or class object
    :param access: allows to select private, public or all methods of
        the object.
    """
    methods = [method for method in dir(obj) if callable(getattr(obj, method))]
    if access == "all":
        pass
    elif access == "private":
        methods = [method for method in methods if method.startswith("_")]
    elif access == "public":
        methods = [method for method in methods if not method.startswith("_")]
    else:
        raise ValueError("Invalid access='%s'" % access)
    return methods
