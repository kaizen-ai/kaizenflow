import collections
import logging
from typing import Any, Dict, Tuple

import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)


def update_nested(dict1: Dict[str, Any], dict2: Dict[str, Any]) -> Dict[str, Any]:
    """
    Update a possibly nested dictionary with leaf values of another.

    This "update" differs from the built-in type in that
      - leaf values of dict1 are not altered unless explicitly changed by dict2
      - Returns the updated dict rather than `None` (makes recursion easy)

    See https://stackoverflow.com/questions/3232943/update-value-of-a-nested-dictionary-of-varying-depth

    :param dict1: Dictionary to be updated. Modified in-place.
    :param dict2: Dictionary with leaf value updates.
    :return: Updated `dict1`
    """
    for k, v in dict2.items():
        if isinstance(v, collections.abc.Mapping):
            dict1[k] = update_nested(dict1.get(k, {}), v)
        else:
            dict1[k] = v
    return dict1


def get_nested_dict_iterator(nested: Dict[Any, Any], path=None) -> Dict[Tuple[Any], Any]:
    """
    Return nested dictionary iterator.

    :param nested: nested dictionary
    :param path: path to top of tree
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