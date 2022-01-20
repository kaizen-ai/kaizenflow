import functools
import hashlib
from typing import Dict, Tuple

import helpers.hdbg as hdbg

def string_to_num_id(string_id: str) -> int:
    """
    Convert string id into a numeric one.

    :param string_id: string id to convert
    :return: numeric id
    """
    # Initialize MD5 algorithm converter and update it with string id.
    converter = hashlib.md5()
    converter.update(string_id.encode("utf-8"))
    # Get hexadecimal numeric id.
    num_id = converter.hexdigest()
    # Convert hexadecimal id to decimal one.
    num_id = int(num_id, 16)
    # Shorten full numeric id to 10 symbols.
    num_id = int(str(num_id)[:10])
    return num_id


@functools.lru_cache()
def build_num_to_string_id_mapping(universe: Tuple[str, ...]) -> Dict[int, str]:
    """
    Build a mapping from numeric ids to string ones.

    :param universe: universe of string ids to convert
    :return: numeric to string ids mapping
    """
    mapping: Dict[int, str] = {}
    for string_id in universe:
        # Convert string id to a numeric one.
        num_id = string_to_num_id(string_id)
        hdbg.dassert_not_in(
            num_id,
            mapping,
            "Collision: id %s for string `%s` already exists",
            num_id,
            string_id,
        )
        mapping[num_id] = string_id
    return mapping
