#!/usr/bin/env python
"""
Encrypt model using Pyarmor.

Usage:
> encrypt_model.py --model_dir dataflow_amp/pipelines/mock1 --target_dir ./encrypted_model --test

# Encrypt a model and save to the same directory.
> encrypt_model.py --model_dir dataflow_amp/pipelines/mock1

# Ecncrypt a model and save to a specific directory.
> encrypt_model.py --model_dir dataflow_amp/pipelines/mock1 --target_dir ./encrypted_model

Import as:

import dev_scripts.encrypt_model as dsenmo
"""
import argparse
import logging
import os
import pathlib
import tempfile

import helpers.hdbg as hdbg
import helpers.hio as hio
import helpers.hparser as hparser
import helpers.hsystem as hsystem

_LOG = logging.getLogger(__name__)


def _encrypt_model(model_dir: str, target_dir: str) -> None:
    """
    Encrypt model using Pyarmor.

    :param model_dir: model directory
    :param target_dir: encrypted model output directory
    """
    hdbg.dassert_dir_exists(model_dir)
    hdbg.dassert_dir_exists(target_dir)
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
    encryption_flow = f"pyarmor-7 obfuscate --restrict=0 --recursive {model_dir} --output {target_dir}"
    docker_cmd = f"docker run --rm -it --workdir {docker_target_dir} --mount {mount} encryption_flow {encryption_flow}"
    _LOG.info("Start running Docker container.")
    hsystem.system(docker_cmd)
    n_files = len(os.listdir(target_dir))
    hdbg.dassert_lt(0, n_files, "No files in target_dir=`%s`", target_dir)
    _LOG.info("Encrypted model successfully stored in target_dir='%s'", target_dir)
    _LOG.info("Remove temporary Dockerfile.")


def _tweak_init(target_dir: str) -> None:
    """
    Add Pyarmor module to `__init__.py` to make sure that encryted model works 
    correctly.

    :param target_dir: encrypted model output directory
    """
    init_file = os.path.join(target_dir, "__init__.py")
    if os.path.exists(init_file):
        data = hio.from_file(init_file)
        lines = "\n".join(
            ["from .pytransform import pyarmor_runtime; pyarmor_runtime()", data]
        )
        hio.to_file(init_file, lines)


def _test_model(model_path: str) -> None:
    """
    Check that model works correctly.
    """
    temp_file_path = "./tmp.encrypt_model.test_model.sh"
    import_path = model_path.lstrip("./")
    import_path = import_path.replace("/", ".")
    script = f'python -c "import {import_path}.mock1_pipeline as f; a = f.Mock1_DagBuilder(); print(a)"'
    # Write testing script to temporary file.
    hio.to_file(
        temp_file_path,
        script
    )
    # Run test inside Docker container
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
    source_dir = pathlib.Path(model_dir)
    model_name = source_dir.stem
    # Create target dir if not exist.
    if target_dir is None:
        encrypted_model_name = "_".join([model_name, "encrypted"])
        target_dir = os.path.join(source_dir.parent, encrypted_model_name)
    hio.create_dir(target_dir, incremental=True)
    _encrypt_model(model_dir, target_dir)
    if os.path.exists(os.path.join(target_dir, "__init__.py")):
        _tweak_init(target_dir)
    if args.test:
        # Test the original model.
        _test_model(model_dir)
        # Test the encrypted model.
        _test_model(target_dir)        


if __name__ == "__main__":
    _main(_parse())
