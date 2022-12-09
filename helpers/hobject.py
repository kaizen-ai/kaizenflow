"""
Import as:

import helpers.hobject as hobject
"""

import logging
import pprint
from typing import Any, List, Optional

import pandas as pd

import helpers.hdbg as hdbg
import helpers.hpandas as hpandas
import helpers.hprint as hprint
import helpers.hstring as hstring

_LOG = logging.getLogger(__name__)
# Mute this module unless we want to debug it.
_LOG.setLevel(logging.INFO)

# #############################################################################
# obj_to_str
# #############################################################################


def _to_skip(is_: bool, mode: str) -> bool:
    hdbg.dassert_in(mode, ("skip", "only", "all"))
    skip = False
    if mode == "skip":
        if is_:
            # Skip all the callables.
            skip = True
    elif mode == "only":
        if not is_:
            # Keep only the callables.
            skip = True
    elif mode == "all":
        # Keep everything.
        skip = False
    else:
        raise ValueError(f"Invalid mode='{mode}'")
    return skip


def _to_skip_callable_attribute(attr_name: Any, mode: str) -> bool:
    is_callable = callable(attr_name)
    skip = _to_skip(is_callable, mode)
    return skip


def _to_skip_private_attribute(attr_name: str, mode: str) -> bool:
    # _Object__hello
    # TODO(gp): This can be improved by passing the name of the object.
    is_dunder = attr_name.startswith("_") and "__" in attr_name
    is_private = not is_dunder and attr_name.startswith("_")
    skip = _to_skip(is_private, mode)
    return skip


def _to_skip_dunder_attribute(attr_name: str, mode: str) -> bool:
    # Is it a double under method, aka dunder?
    is_dunder = attr_name.startswith("_") and "__" in attr_name
    skip = _to_skip(is_dunder, mode)
    return skip


def _to_skip_attribute(
    attr_name: Any,
    attr_value: Any,
    callable_mode: str,
    private_mode: str,
    dunder_mode: str,
    attr_names_to_skip: Optional[List[str]],
) -> bool:
    # Check whether the attribute is one that was requested explicitly to skip.
    if attr_names_to_skip is not None:
        if attr_name in attr_names_to_skip:
            skip = True
            return skip
    # Handle callable methods.
    skip = _to_skip_callable_attribute(attr_value, callable_mode)
    if skip:
        _LOG.debug("Skip callable")
        return skip
    # Handle private methods.
    skip = _to_skip_private_attribute(attr_name, private_mode)
    if skip:
        _LOG.debug("Skip private")
        return skip
    # Handle dunder methods.
    skip = _to_skip_dunder_attribute(attr_name, dunder_mode)
    if skip:
        _LOG.debug("Skip dunder")
        return skip
    return False


def _type_to_str(attr_value: str) -> str:
    type_as_str = str(type(attr_value))
    type_as_str = hstring.remove_prefix(type_as_str, "<class '")
    type_as_str = hstring.remove_suffix(type_as_str, "'>")
    type_as_str = f"<{type_as_str}>"
    return type_as_str


# #############################################################################


def _attr_to_str(attr_value: Any, print_type: bool) -> str:
    _LOG.debug("type(attr_value)=%s", type(attr_value))
    if isinstance(attr_value, pd.DataFrame):
        res = f"pd.df({attr_value.shape}"
    elif isinstance(attr_value, pd.Series):
        res = f"pd.srs({attr_value.shape}"
    elif isinstance(attr_value, dict):
        res = str(attr_value)
    else:
        res = str(attr_value)
    if print_type:
        res += " " + _type_to_str(attr_value)
    return res


def obj_to_str(
    obj: Any,
    *,
    attr_mode: str = "__dict__",
    sort: bool = False,
    print_type: bool = False,
    callable_mode: str = "skip",
    private_mode: str = "skip",
    dunder_mode: str = "skip",
    attr_names_to_skip: Optional[List[str]] = None,
) -> str:
    """
    Print attributes of an object.

    An object is printed as name of the class and the attributes, e.g.,
    ```
    _Object:
      a='False'
      b='hello'
      c='3.14'
    ```

    :param attr_mode: use `__dict__` or `dir()`
        - It doesn't seem to make much difference
    :param print_type: print the type of the attribute
    :param callable_mode: how to handle attributes that are callable (i.e.,
        methods)
        - `skip`: skip the callable methods
        - `only`: print only the callable methods
        - `all`: always print
    :param private_mode: how to handle private attributes. Same params as
        `callable_mode`
    :param dunder_mode: how to handle double under attributes. Same params as
        `callable_mode`
    :param attr_names_to_skip: a list of attributes (e.g., private, callable, dunder)
        to skip. This is used to avoid to print data that is redundant (e.g., a
        cached value)
    """
    ret = []
    if attr_mode == "__dict__":
        values = obj.__dict__
        if sort:
            values = sorted(values)
        for attr_name in values:
            attr_value = obj.__dict__[attr_name]
            skip = _to_skip_attribute(
                attr_name,
                attr_value,
                callable_mode,
                private_mode,
                dunder_mode,
                attr_names_to_skip,
            )
            _LOG.debug(
                "attr_name=%s attr_value=%s -> skip", attr_name, attr_value, skip
            )
            if skip:
                continue
            #
            out = f"{attr_name}=" + _attr_to_str(attr_value, print_type)
            ret.append(out)
    elif attr_mode == "dir":
        values = dir(obj)
        if sort:
            values = sorted(values)
        for attr_name in values:
            attr_value = getattr(obj, attr_name)
            skip = _to_skip_attribute(
                attr_name,
                attr_value,
                callable_mode,
                private_mode,
                dunder_mode,
                attr_names_to_skip,
            )
            _LOG.debug(
                "attr_name=%s attr_value=%s -> skip", attr_name, attr_value, skip
            )
            if skip:
                continue
            #
            out = f"{attr_name}=" + _attr_to_str(attr_value, print_type)
            ret.append(out)
    else:
        hdbg.dassert(f"Invalid attr_mode='{attr_mode}'")
    #
    txt = hprint.to_object_str(obj) + "="
    txt += "(" + ", ".join(ret) + ")"
    return txt


# #############################################################################


def _attr_to_repr(attr_name: Any, attr_value: Any, print_type: bool) -> str:
    _LOG.debug("type(attr_value)=%s", type(attr_value))
    if isinstance(attr_value, (pd.DataFrame, pd.Series)):
        attr_value_as_str = hpandas.df_to_str(attr_value)
    elif isinstance(attr_value, dict):
        attr_value_as_str = pprint.pformat(attr_value)
    else:
        attr_value_as_str = repr(attr_value)
    if len(attr_value_as_str.split("\n")) > 1:
        # The string representing the attribute value spans multiple lines, so print
        # like:
        # ```
        # attr_name= (type)
        #   attr_value
        # ```
        out = f"{attr_name}="
        if print_type:
            out += " " + _type_to_str(attr_value)
        out += "\n" + hprint.indent(attr_value_as_str)
    else:
        # The string representing the attribute value is a single line, so print
        # like:
        # ```
        # attr_name='attr_value' (type)
        # ```
        out = f"{attr_name}='{str(attr_value)}'"
        if print_type:
            out += " " + _type_to_str(attr_value)
    return out


# TODO(gp): Merge the code with obj_to_repr() using a switch for the different
#  code.
def obj_to_repr(
    obj: Any,
    *,
    attr_mode: str = "__dict__",
    sort: bool = False,
    print_type: bool = False,
    callable_mode: str = "skip",
    private_mode: str = "skip",
    dunder_mode: str = "skip",
    attr_names_to_skip: Optional[List[str]] = None,
) -> str:
    """
    Same interface and behavior as `obj_to_str()`.

    Use `_attr_to_repr()` instead of a simple `attr_name = attr_value` like in
    `obj_to_str()`.
    """
    ret = []
    if attr_mode == "__dict__":
        values = obj.__dict__
        if sort:
            values = sorted(values)
        for attr_name in values:
            attr_value = obj.__dict__[attr_name]
            skip = _to_skip_attribute(
                attr_name,
                attr_value,
                callable_mode,
                private_mode,
                dunder_mode,
                attr_names_to_skip,
            )
            _LOG.debug(
                "attr_name=%s attr_value=%s -> skip", attr_name, attr_value, skip
            )
            if skip:
                continue
            #
            out = _attr_to_repr(attr_name, attr_value, print_type)
            ret.append(out)
    elif attr_mode == "dir":
        values = dir(obj)
        if sort:
            values = sorted(values)
        for attr_name in values:
            attr_value = getattr(obj, attr_name)
            skip = _to_skip_attribute(
                attr_name,
                attr_value,
                callable_mode,
                private_mode,
                dunder_mode,
                attr_names_to_skip,
            )
            _LOG.debug(
                "attr_name=%s attr_value=%s -> skip", attr_name, attr_value, skip
            )
            if skip:
                continue
            #
            out = _attr_to_repr(attr_name, attr_value, print_type)
            ret.append(out)
    else:
        hdbg.dassert(f"Invalid attr_mode='{attr_mode}'")
    #
    txt = []
    txt.append(hprint.to_object_repr(obj) + ":")
    txt.append(hprint.indent("\n".join(ret)))
    return "\n".join(txt)


class PrintableMixin:
    """
    Implement default `__str__()` and `__repr__()` printing the state of an
    object.

    - `str()` is:
        - to be readable
        - used for creating output for end user
    - `repr()` is
        - to be unambiguous
        - used for debugging and development.

    These methods can be overridden with more specific methods, if needed.
    """

    def __str__(
        self,
        attr_names_to_skip: Optional[List[str]] = None,
    ) -> str:
        return obj_to_str(
            self,
            print_type=True,
            private_mode="all",
            attr_names_to_skip=attr_names_to_skip,
        )

    def __repr__(
        self,
        attr_names_to_skip: Optional[List[str]] = None,
    ) -> str:
        return obj_to_repr(
            self,
            print_type=True,
            private_mode="all",
            attr_names_to_skip=attr_names_to_skip,
        )


# #############################################################################


def test_object_signature(
    self_: Any, obj: Any, *, remove_lines_regex: Optional[str] = None
) -> None:
    txt = []
    txt.append(hprint.frame("str:"))
    txt.append(str(obj))
    txt.append(hprint.frame("repr:"))
    txt.append(repr(obj))
    txt = "\n".join(txt)
    # Remove certain lines, if needed.
    if remove_lines_regex:
        txt = hprint.filter_text(remove_lines_regex, txt)
    #
    self_.check_string(txt, purify_text=True)