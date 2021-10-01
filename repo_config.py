"""
This file contains info specific of this repo.
"""

from typing import Dict


def get_repo_map() -> Dict[str, str]:
    """
    Return a mapping of short repo name -> long repo name.
    """
    repo_map: Dict[str, str] = {"cm": "cryptomtc/cmamp"}
    return repo_map


def get_host_name() -> str:
    return "github.com"
