import collections
import logging
from typing import Any, Dict

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