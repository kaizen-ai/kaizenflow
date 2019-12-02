"""
Import as:

import helpers.list as hlist
"""

from typing import List

import helpers.dbg as dbg


def assert_single_element_and_return(list_: List[str]) -> str:
    """
    Asserts that list `l` has a single element and returns it.

    :param list_: list
    :return: returns the unique element of the list
    """
    dbg.dassert_isinstance(list_, list)
    dbg.dassert_eq(len(list_), 1, "List has %d elements!", len(list_))
    return list_[0]


def find_duplicates(list_: List) -> List:
    """
    Find the elements duplicated in a list.
    """
    dbg.dassert_isinstance(list_, list)
    # Count the occurrences of each element of the seq.
    # TODO(gp): Consider replacing with pd.Series.value_counts.
    set_l = set(list_)
    v_to_num = [(v, list_.count(v)) for v in set_l]
    # Build list of elems with duplicates.
    res = [v for v, n in v_to_num if n > 1]
    return res


def remove_duplicates(list_: List) -> List:
    """
    Remove the elements duplicated in a list, without changing the order.
    """
    dbg.dassert_isinstance(list_, list)
    list_out = []
    set_l = set()
    for v in list_:
        if v not in set_l:
            set_l.add(v)
            list_out.append(v)
    return list_out
