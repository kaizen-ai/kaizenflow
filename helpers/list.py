"""
Import as:

import helpers.list as hlist
"""

# TODO(gp): -> `list_helpers.py` or maybe `python_helpers` with all code about
#  general Python functions.

from typing import Any, List, Set

import helpers.dbg as dbg


# TODO(gp): -> return_single_element, return_single_element_or_assert?
def assert_single_element_and_return(list_: List[Any]) -> Any:
    """
    Assert that the passed list has a single element and return that single
    element.

    :return: return the unique element in the list
    """
    dbg.dassert_isinstance(list_, list)
    dbg.dassert_eq(len(list_), 1, "List has %d elements!", len(list_))
    return list_[0]


def find_duplicates(list_: List[Any]) -> List[Any]:
    """
    Find the elements duplicated in a list.
    """
    dbg.dassert_isinstance(list_, list)
    # Count the occurrences of each element of the seq.
    set_l = set(list_)
    v_to_num = [(v, list_.count(v)) for v in set_l]
    # Build list of elems with duplicates.
    res = [v for v, n in v_to_num if n > 1]
    return res


def remove_duplicates(list_: List[Any]) -> List[Any]:
    """
    Remove the elements duplicated in a list, without changing the order.
    """
    dbg.dassert_isinstance(list_, list)
    list_out = []
    set_l: Set[Any] = set()
    for v in list_:
        if v not in set_l:
            set_l.add(v)
            list_out.append(v)
    return list_out
