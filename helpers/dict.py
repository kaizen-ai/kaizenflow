"""
Import as:

import helpers.dict as dct
"""

import collections
import logging
from typing import Any, Dict, Iterable, Optional, Tuple

_LOG = logging.getLogger(__name__)


def get_nested_dict_iterator(
        nested: Dict[Any, Any], path: Optional[Iterable[Any]] = None,
) -> Dict[Tuple[Any], Any]:
    """
    Return nested dictionary iterator that iterates in a depth-first fashion.

    :param nested: nested dictionary
    :param path: path to node to start the visit from or `None` to start from
        the root
    :return: path to leaf node, value
    """
    if path is None:
        path = []
    for key, value in nested.items():
        local_path = path + [key]
        if isinstance(value, collections.abc.Mapping):
            yield from get_nested_dict_iterator(value, local_path)
        else:
            yield local_path, value


def extract_leaf_values(nested, key):
    """
    Extract leaf values with key matching `key`.

    :param nested: nested dictionary
    :param key: leaf key value to match
    :return: dict with key = path as tuple, value = leaf value
    """
    d = {}
    for item in get_nested_dict_iterator(nested):
        if item[0][-1] == key:
            d[tuple(item[0])] = item[1]
    return d


def flatten_nested_dict(nested):
    d = {}
    for item in get_nested_dict_iterator(nested):
        d[".".join(item[0])] = item[1]
    return d
