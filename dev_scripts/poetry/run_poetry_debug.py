# TODO(Grisha): @Nikola it should become executable script.

import os
from typing import List

import helpers.hdbg as hdbg
import helpers.hgit as hgit
import helpers.hio as hio
import helpers.hsystem as hsystem


def get_necessary_packages() -> List[str]:
    necessary_packages = [
        "python = \"^3.8\"",
        "awscli = \"1.22.17\"",
        "jupyter = \"*\"",
        "jupyter_contrib_nbextensions = \"*\"",
        "jupyter_nbextensions_configurator = \"*\"",
        "matplotlib = \"*\"",
        "networkx = \"*\"",
        "pandas = \"*\"",
        "psycopg2-binary = \"*\"",
        "pyarrow = \"*\"",
        "pytest = \"*\"",
        "pytest-cov = \"*\"",
        "pytest-instafail = \"*\"",
        "pytest-rerunfailures = \"*\"",
        "pytest-timeout = \"*\"",
        "pytest-xdist = \"*\"",
        "python-dotenv = \"*\"",
        "pywavelets = \"*\"",
        "s3fs = \"*\"",
        "seaborn = \"*\"",
        "sklearn = \"*\"",
        "statsmodels = \"*\"",
        "tqdm = \"*\"",
    ]
    return necessary_packages


def get_optional_packages() -> List[str]:
    optional_packages = [
        "boto3 = \"*\"",
        "invoke = \"*\"",
        "jsonpickle = \"*\"",
        "moto = \"*\"",
        "psutil = \"*\"",
        "pygraphviz = \"*\"",
        "requests = \"*\"",
    ]
    return optional_packages


def write_pyproject_toml(packages: List[str], dir_name: str) -> None:
    beginning_of_file = """
    [tool.poetry]
    name = "amp"
    version = "1.0.0"
    description = ""
    authors = ["Your Name <you@example.com>"]

    [tool.poetry.dependencies]
    """
    end_of_file = """
    
    [tool.poetry.dev-dependencies]

    [build-system]
    requires = ["poetry-core>=1.0.0"]
    build-backend = "poetry.core.masonry.api"
    """
    packages = "\n".join(packages)
    file_content = "".join([beginning_of_file, packages, end_of_file])
    file_path = os.path.join(dir_name, "pyproject.toml")
    hio.to_file(file_path, file_content)

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


def poetry_add_python_packages(python_packages: List[str]) -> None:
    for python_package in python_packages:
        poetry_add_package_cmd = get_add_package_poetry_cmd(python_package)
        # Add every package in a provided list.
        hsystem.system(poetry_add_package_cmd)


def run_poetry_debug(include_optional_packages: bool) -> None:
    # Get Python packages to debug.
    python_packages = get_python_packages(include_optional_packages)
    poetry_run_cmd = get_run_poetry_cmd()
    for idx, _ in enumerate(python_packages):
        # We want to incrementally add one package and see if it works.
        current_packages = python_packages[:idx]
        poetry_add_python_packages(current_packages)
        # TODO(Grisha): @Nikola we should also measure how much time it takes.
        # TODO(Grisha): @Nikola let's abort the script if running time > 30 minutes (should be a parameter).
        # TODO(Grisha): @Nikola capture output and compute length.
        hsystem.system(poetry_run_cmd)
        # TODO(Grisha): @Nikola write stats in a file:
        #   - what are the packages
        #   - did it converge? (run successfully)
        #   - how much time did it take?
        #   - how large is the output? i.e. length of string.




