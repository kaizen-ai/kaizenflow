#!/usr/bin/env python

import argparse
import logging
import os
from typing import List

import helpers.hdbg as hdbg
import helpers.hgit as hgit
import helpers.hio as hio
import helpers.hparser as hparser
import helpers.hprint as hprint
import helpers.hsystem as hsystem

_LOG = logging.getLogger(__name__)


def get_necessary_packages() -> List[str]:
    necessary_packages = [
        'python = "^3.8"',
        'pandas = "*"',
        'jupyter = "*"',
        'awscli = "1.22.17"',
        'jupyter_contrib_nbextensions = "*"',
        'jupyter_nbextensions_configurator = "*"',
        'matplotlib = "*"',
        'networkx = "*"',
        'psycopg2-binary = "*"',
        'pyarrow = "*"',
        'pytest = "*"',
        'pytest-cov = "*"',
        'pytest-instafail = "*"',
        'pytest-rerunfailures = "*"',
        'pytest-timeout = "*"',
        'pytest-xdist = "*"',
        'python-dotenv = "*"',
        'pywavelets = "*"',
        's3fs = "*"',
        'seaborn = "*"',
        'sklearn = "*"',
        'statsmodels = "*"',
        'tqdm = "*"',
    ]
    return necessary_packages


def get_optional_packages() -> List[str]:
    optional_packages = [
        'boto3 = "*"',
        'invoke = "*"',
        'jsonpickle = "*"',
        'moto = "*"',
        'psutil = "*"',
        'pygraphviz = "*"',
        'requests = "*"',
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
    file_content = "".join(
        [
            hprint.dedent(beginning_of_file),
            hprint.dedent(packages),
            "\n",
            hprint.dedent(end_of_file),
        ]
    )
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


def run_poetry_cmd(dir_path: str) -> None:
    """
    Poetry run with verbose lock command.

    :param dir_path: path of directory where command is run
    :return:
    """
    # Prepare poetry lock command.
    cmd = f"cd {dir_path}; poetry lock -vv"
    _LOG.info("Resolving poetry dependencies cdm=`%s`", cmd)
    # Prepare log file.
    log_file_name = "poetry.log"
    log_file_path = os.path.join(dir_path, log_file_name)
    # Run poetry lock command.
    hsystem.system(cmd, suppress_output=False, output_file=log_file_path)


def _run_poetry_cmd_wrapper(
    dir_name: str, python_packages: list, last_package: str = ""
) -> None:
    """
    Simple poetry command wrapper that can be called multiple times.

    :param dir_name: directory name based on debug mode
    :param python_packages: list of packages to be written in .toml file
    :param last_package: last package in `pyproject.toml` that is useful for
        creating different log files in incremental run
    :return:
    """
    # Prepare base dir depending on debug mode.
    dir_path = os.path.join(get_debug_poetry_dir(), dir_name)
    # Use clean package name, if package name is provided.
    if last_package:
        last_package = last_package.split(" ")[0]
        dir_path = os.path.join(dir_path, last_package)
    # Write .toml files.
    write_poetry_toml_file(dir_path)
    write_pyproject_toml(python_packages, dir_path)
    # TODO(Grisha): @Nikola we should terminate a script if it is not finished
    #  within 30 minutes (could be a param).
    # Run.
    run_poetry_cmd(dir_path)


def get_debug_poetry_dir() -> str:
    amp_path = hgit.get_amp_abs_path()
    poetry_debug_dir = os.path.join(amp_path, "dev_scripts/poetry")
    hdbg.dassert_dir_exists(poetry_debug_dir)
    return poetry_debug_dir


def run_poetry_debug(debug_mode: str) -> None:
    """
    Run poetry debug with various options.

    :param debug_mode:
        - `necessary` - list of necessary packages in one shot
        - `necessary_incremental` - list of necessary packages one by one
            - i.e. add 1 package, run poetry, save the output, add another one, run poetry, etc.
        - `optional` - list of necessary and optional packages in one shot
        - `optional_incremental` - list of necessary packages in one shot,
            while optional are run one by one
    :return
    """
    # Get Python packages to debug.
    necessary_packages = get_necessary_packages()
    optional_packages = []
    if debug_mode in ("optional", "optional_incremental"):
        optional_packages.extend(get_optional_packages())
    all_packages = necessary_packages + optional_packages
    # Pick desired debug option.
    dir_name = debug_mode
    if debug_mode == "necessary_incremental":
        # Add necessary packages one by one.
        current_necessary_packages = []
        for necessary_package in necessary_packages:
            current_necessary_packages.append(necessary_package)
            _LOG.info(
                "Adding necessary incremental packages=`%s`",
                current_necessary_packages,
            )
            _run_poetry_cmd_wrapper(
                dir_name,
                current_necessary_packages,
                last_package=necessary_package,
            )
    elif debug_mode == "optional_incremental":
        # Add optional packages one by one, after necessary ones in one shot.
        current_optional_packages = []
        for optional_package in optional_packages:
            current_optional_packages.append(optional_package)
            _LOG.info(
                "Adding optional incremental packages=`%s`",
                current_optional_packages,
            )
            _run_poetry_cmd_wrapper(
                dir_name,
                necessary_packages + current_optional_packages,
                last_package=optional_package,
            )
    else:
        # Add packages in one shot.
        _LOG.info("Adding packages in one shot=`%s`", all_packages)
        _run_poetry_cmd_wrapper(dir_name, all_packages)


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--debug_mode",
        action="store",
        type=str,
        required=True,
        help="Run poetry with desired list of packages",
    )
    hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    debug_mode = args.debug_mode
    valid_debug_modes = (
        "necessary",
        "necessary_incremental",
        "optional",
        "optional_incremental",
    )
    hdbg.dassert_in(debug_mode, valid_debug_modes)
    run_poetry_debug(debug_mode)


if __name__ == "__main__":
    _main(_parse())
