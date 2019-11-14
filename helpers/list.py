from typing import List

import helpers.dbg as dbg


def assert_single_element_and_return(l: List[str]) -> str:
    """
    Asserts that list `l` has a single element and returns it.

    :param l: list
    :return: returns the unique element of the list
    """
    dbg.dassert_isinstance(l, list)
    dbg.dassert_eq(len(l), 1, "List has %d elements!", len(l))
    return l[0]