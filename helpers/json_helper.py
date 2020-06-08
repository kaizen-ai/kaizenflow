"""
Import as:

import helpers.json_helper as json_help
"""

import json


def to_json(obj: dict, file_name: str, key="w") -> None:
    """
    Json implementation for writing to file
    :param obj: data for writing
    :param file_name: name of file
    :param key: w == writing
    :return:
    """
    if type(key) is not str:
        raise ValueError("Key must be str")
    with open(file_name, key) as outfile:
        json.dump(obj, outfile)


def from_json(file_name, key="r") -> dict:
    """
    Json implementation for reading from file
    :param file_name: name of file
    :param key: r == reading
    :return:
    Dict with data
    """
    if type(key) is not str:
        raise ValueError("Key must be str")
    with open(file_name, key) as f:
        data = json.loads(f.read())
    return data
