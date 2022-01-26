import os
from typing import List

import helpers.hdbg as hdbg
import helpers.hgit as hgit
import helpers.hio as hio
import helpers.hsystem as hsystem

def get_add_package_poetry_cmd(package: str) -> str:
    cmd = f"poetry add {package}"
    return cmd

def get_run_poetry_cmd() -> str:
    # TODO(Grisha): @Nikola make sure you are in `dev_scripts/poetry` dir.
    cmd = "poetry lock -vv"
    return cmd


def get_debug_poetry_dir() -> str:
    amp_path = hgit.get_amp_abs_path()
    poetry_debug_dir = os.path.join(amp_path, "dev_scripts/poetry")
    hdbg.dassert_dir_exists(poetry_debug_dir)
    return poetry_debug_dir


def load_python_packages(packages_type: str) -> List[str]:
    file_name = ""
    if packages_type == "necessary":
        file_name = "necessary_packages"
    elif packages_type == "optional":
        file_name = "optional_packages"
    else:
        raise ValueError(f"unsupported packages type {packages_type}")
    poetry_debug_dir = get_debug_poetry_dir()
    packages_file_path = os.path.join(poetry_debug_dir, file_name)
    packages = hio.from_file(packages_file_path)
    hdbg.dassert_file_exists(packages_file_path)
    return packages


def get_python_packages(include_optional_packages: bool) -> List[str]:
    packages = load_python_packages("necessary")
    if include_optional_packages:
        optional_packages = load_python_packages("optional")
        packages.extend(optional_packages)
    return packages


def run_poetry_debug(include_optional_packages: bool) -> str:
    python_packages = get_python_packages(include_optional_packages)
    poetry_run_cmd = get_run_poetry_cmd()
    for package in python_packages:
        poetry_add_package_cmd = get_add_package_poetry_cmd(package)


