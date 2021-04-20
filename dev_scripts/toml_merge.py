#!/usr/bin/env python

"""
Merge two or more

> toml_merge.py \
    --in devops/docker_build/pyproject.toml \
    --in amp/devops/docker_build/pyproject.toml \
    --out pyproject.toml
"""

import argparse
import logging

import helpers.dbg as dbg
import helpers.parser as prsr
import helpers.io_ as io_

_LOG = logging.getLogger(__name__)

from typing import Any, Dict

import copy
import collections
import toml
import pprint

_DepDict = Dict[str, str]

def _update(dict_merged: _DepDict, dict_new: _DepDict) -> _DepDict:
    for k, v in dict_new.items():
        v = dict_new[k]
        if k in dict_merged:
            if v != dict_merged[k]:
                raise ValueError("Key '%s' is assigned to different values"
                                 % (k, v, dict_merged[k]))
        else:
            dict_merged[k] = v
    return dict_merged


def _merge_deps(dict1: _DepDict, dict2: _DepDict) -> _DepDict:
    dict_merged = collections.OrderedDict()
    dict_merged = _update(dict_merged, dict1)
    dict_merged = _update(dict_merged, dict2)
    return dict_merged


def _merge_toml(pyproj1: Dict, pyproj2: Dict) -> Dict:
    for key in ["dependencies", "dev-dependencies"]:
        pyproj1_deps = pyproj1["tool"]["poetry"].get(key, {})
        pyproj2_deps = pyproj1["tool"]["poetry"].get(key, {})
        pyproj["tool"]["poetry"][key] = _merge_deps(pyproj1_deps, pyproj2_deps)
    dict_ = toml.dumps(pyproj)
    return dict_


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--in", action="append", help="Files to read")
    parser.add_argument("--out", action="store", help="File to write")
    prsr.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    #
    #file_name = "/Users/saggese/src/lemonade/devops/docker_build/pyproject.toml"
    io_.
    pyproj1 = toml.load(file_name)
    _LOG.debug(pprint.pformat(pyproj1))
    pyproj = copy.deepcopy(pyproj1)

    file_name = "/Users/saggese/src/lemonade/amp/devops/docker_build/pyproject.toml"
    pyproj2 = toml.load(file_name)
    print(pprint.pformat(pyproj2))

    # > ../../dev_scripts/toml_merge.py
    # {'build-system': {'build-backend': 'poetry.masonry.api',
    #                   'requires': ['poetry>=0.12']},
    #  'tool': {'poetry': {'authors': [''],
    #                      'dependencies': {'boto3': '*',
    #                                       'bs4': '*',
    #                                       'flaky': '*',
    #                                       'fsspec': '*',
    _merge_toml(pyproj1, pyproj2)


if __name__ == "__main__":
    _main(_parse())
