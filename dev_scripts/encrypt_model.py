#!/usr/bin/env python
"""
Encrypt model using Pyarmor.

Usage:
> dev_scripts/encrypt_model.py --model_dir dataflow_amp/pipelines/mock1 --test

Import as:

import dev_scripts.encrypt_model as dsenmo
"""
import argparse
import logging
import os
import pathlib
import tempfile
import stat

import helpers.hdbg as hdbg
import helpers.hio as hio
import helpers.hparser as hparser
import helpers.hsystem as hsystem

_LOG = logging.getLogger(__name__)


def _encrypt_model(model_dir: str, target_dir: str) -> str:
    """
    Encrypt model using Pyarmor.

    :param model_dir: source model directory
    :param target_dir: encrypted model output directory
    :return: encrypted model directory
    """
    hdbg.dassert_dir_exists(model_dir)
    hdbg.dassert_dir_exists(target_dir)
    # Create encrypted model directory.
    model_path = pathlib.Path(model_dir)
    model_name = model_path.stem
    encrypted_model_name = "_".join(["encrypted", model_name])
    encrypted_model_dir = os.path.join(target_dir, encrypted_model_name)
    hio.create_dir(encrypted_model_dir, incremental=True)
    # Create temporary Dockerfile.
    with tempfile.NamedTemporaryFile(suffix=".Dockerfile") as temp_dockerfile:
        temp_dockerfile.write(
            b"""
                FROM python:3.8
                RUN pip install pyarmor
            """
        )
        temp_dockerfile.flush()
        cmd = f"docker build -f {temp_dockerfile.name} -t encryption_flow ."
        hsystem.system(cmd)
    # Run Docker container to encrypt the model.
    work_dir = os.getcwd()
    docker_target_dir = "/app"
    mount = f"type=bind,source={work_dir},target={docker_target_dir}"
    encryption_flow = f"pyarmor-7 obfuscate --restrict=0 --recursive {model_dir} --output {encrypted_model_dir}"
    docker_cmd = f"docker run --rm -it --workdir {docker_target_dir} --mount {mount} encryption_flow {encryption_flow}"
    _LOG.info("Start running Docker container.")
    hsystem.system(docker_cmd)
    n_files = len(os.listdir(encrypted_model_dir))
    hdbg.dassert_lt(0, n_files, "No files in encrypted_model_dir=`%s`", encrypted_model_dir)
    _LOG.info("Encrypted model successfully stored in encrypted_model_dir='%s'", encrypted_model_dir)
    # Make encrypted model files accessible by any user.
    cmd = f"sudo chmod -R 777 {encrypted_model_dir}"
    hsystem.system(cmd)
    _LOG.info("Remove temporary Dockerfile.")
    return encrypted_model_dir


def _tweak_init(encrypted_model_dir: str) -> None:
    """
    Add Pyarmor module to `__init__.py` to make sure that encrypted model works 
    correctly.

    :param encrypted_model_dir: encrypted model directory
    """
    init_file = os.path.join(encrypted_model_dir, "__init__.py")
    if os.path.exists(init_file):
        data = hio.from_file(init_file)
        lines = "\n".join(
            ["from .pytransform import pyarmor_runtime; pyarmor_runtime()", data]
        )
        hio.to_file(init_file, lines)


def _test_model(model_dir: str) -> None:
    """
    Check that model works correctly.
    """
    temp_file_path = "./tmp.encrypt_model.test_model.sh"
    import_path = model_dir.lstrip("./")
    import_path = import_path.replace("/", ".")
    script = f'python -c "import {import_path}.mock1_pipeline as f; a = f.Mock1_DagBuilder(); print(a)"'
    # Write testing script to temporary file.
    hio.to_file(temp_file_path, script)
    # Run test inside Docker container.
    cmd = f"invoke docker_cmd -c 'bash {temp_file_path}'"
    (_, output) = hsystem.system_to_string(cmd)
    _LOG.debug(output)
    os.remove(temp_file_path)


# #############################################################################


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--model_dir",
        required=True,
        type=str,
        help="Source model directory"
    )
    parser.add_argument(
        "--target_dir",
        required=False,
        default=None,
        type=str,
        help="Encrypted model output directory",
    )
    parser.add_argument(
        "--test",
        required=False,
        action="store_true",
        help="Run testing"
    )
    hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    #
    model_dir = args.model_dir
    hdbg.dassert_dir_exists(model_dir)
    target_dir = args.target_dir
    if target_dir is not None:
        hdbg.dassert_dir_exists(target_dir)
    else:
        model_path = pathlib.Path(model_dir)
        target_dir = str(model_path.parent)
    encrypted_model_dir = _encrypt_model(model_dir, target_dir)
    # Tweak `__init__.py` file.
    encrypted_init_file = os.path.join(encrypted_model_dir, "__init__.py")
    if os.path.exists(encrypted_init_file):
        _tweak_init(encrypted_model_dir)
    if args.test:
        _test_model(model_dir)
        _test_model(encrypted_model_dir)        


if __name__ == "__main__":
    _main(_parse())
