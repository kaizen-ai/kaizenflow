"""
Import as:

import helpers.docker_manager as hdocmana
"""

import argparse
from typing import Dict, List

import docker


def get_volumes(
    name: str,  # pylint: disable=unused-argument
) -> List[Dict[str, str]]:
    client = docker.from_env()
    container = client.containers.get("postgres_service")
    output: List[Dict[str, str]] = container.attrs["Mounts"]
    return output


def get_source_from(name: str) -> str:
    s = ""
    for i in get_volumes(name):
        s += i["Source"] + "\n"
    return s


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--get_volumes", required=False, type=str, action="store")
    args = parser.parse_args()
    # _main(args)
    if args.get_volumes:
        print((get_source_from(args.get_volumes)))
