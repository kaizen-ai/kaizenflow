#!/usr/bin/env python

import argparse
import logging
import os
from typing import List

import helpers.hdbg as hdbg
import helpers.hgit as hgit
import helpers.hio as hio
import helpers.hprint as hprint
import helpers.hsystem as hsystem
import helpers.hparser as hparser


_LOG = logging.getLogger(__name__)


def get_necessary_packages() -> List[str]:
    necessary_packages = [
        "python = \"^3.8\"",
        "pandas = \"*\"",
        "jupyter = \"*\"",
        "awscli = \"1.22.17\"",
        "jupyter_contrib_nbextensions = \"*\"",
        "jupyter_nbextensions_configurator = \"*\"",
        "matplotlib = \"*\"",
        "networkx = \"*\"",
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
    file_content = "".join([hprint.dedent(beginning_of_file), hprint.dedent(packages), "\n", hprint.dedent(end_of_file)])
    poetry_debug_dir = get_debug_poetry_dir()
    file_path = os.path.join(poetry_debug_dir, dir_name, "pyproject.toml")
    _LOG.info("Writing `pyproject.toml` to file=`%s`", file_path)
    hio.to_file(file_path, file_content)


def write_poetry_toml_file(dir_name: str) -> None:
    file_content = """
    cache-dir = "tmp.pypoetry"
    experimental.new-installer = true
    installer.parallel = true
    # We don't want poetry to automatically create / manage virtual environment.
    virtualenvs.create = false
    virtualenvs.in-project = true
    """
    file_content = hprint.dedent(file_content)
    poetry_debug_dir = get_debug_poetry_dir()
    file_path = os.path.join(poetry_debug_dir, dir_name, "poetry.toml")
    _LOG.info("Writing `poetry.toml` to file=`%s`", file_path)
    hio.to_file(file_path, file_content)


def run_poetry_cmd() -> None:
    dir_name = os.path.join(get_debug_poetry_dir(), "test")
    cmd = f"cd {dir_name}; poetry lock -vv"
    _LOG.info("Resolving poetry dependencies cdm=`%s`", cmd)
    hsystem.system(cmd, suppress_output=False)


def get_debug_poetry_dir() -> str:
    amp_path = hgit.get_amp_abs_path()
    poetry_debug_dir = os.path.join(amp_path, "dev_scripts/poetry")
    hdbg.dassert_dir_exists(poetry_debug_dir)
    return poetry_debug_dir


def run_poetry_debug() -> None:
    # Get Python packages to debug.
    python_packages = get_necessary_packages()[:2]
    _LOG.info("Adding packages=`%s`", python_packages)
    dir_name = "test"
    write_poetry_toml_file(dir_name)
    write_pyproject_toml(python_packages, dir_name)
    run_poetry_cmd()


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    run_poetry_debug()

if __name__ == "__main__":
    _main(_parse())


