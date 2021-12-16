"""
This file contains info specific of this repo.
"""

from typing import Dict, List


def get_repo_map() -> Dict[str, str]:
    """
    Return a mapping of short repo name -> long repo name.
    """
    repo_map: Dict[str, str] = {"cm": "cryptokaizen/cmamp"}
    return repo_map


def get_extra_amp_repo_sym_name() -> str:
    return "cryptokaizen/cmamp"


# TODO(gp): -> get_gihub_host_name
def get_host_name() -> str:
    return "github.com"


def get_invalid_words() -> List[str]:
    return []


def get_docker_base_image_name() -> str:
    """
    Return a base name for docker image.
    """
    base_image_name = "cmamp"
    return base_image_name


def has_dind_support() -> bool:
    """
    Return whether this repo image supports Docker-in-Docker.
    """
    return True
