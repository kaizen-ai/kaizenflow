#!/usr/bin/env python
"""
Encrypt model using Pyarmor.

Usage:
> encrypt_model.py --source_dir model_source_dir --target_dir encrypted_model_output_dir

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


def _encrypt_model(source_dir: str, target_dir: str) -> None:
    """
    Encrypt model using Pyarmor.

    :param source_dir: model directory
    :param target_dir: encrypted model output directory
    """
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
    encryption_flow = f"pyarmor-7 obfuscate --restrict=0 --recursive {source_dir} --output {target_dir}"
    docker_cmd = f"docker run --rm -it --workdir {docker_target_dir} --mount {mount} encryption_flow {encryption_flow}"
    _LOG.info("Start running Docker container.")
    hsystem.system(docker_cmd)
    _LOG.info("Encrypted model successfully stored in %s.", target_dir)
    _LOG.info("Remove temporary Dockerfile.")


def _tweak_init(target_dir: str) -> None:
    """
    Add necessary Pyarmor module into `__init__.py` to ensure encryted model
    works as expected.

    :param target_dir: encrypted model output directory
    """
    _LOG.info("Start tweaking __init__.py.")
    init_file = os.path.join(target_dir, "__init__.py")
    if os.path.exists(init_file):
        data = hio.from_file(init_file)
        lines = "\n".join(
            ["from .pytransform import pyarmor_runtime; pyarmor_runtime()", data]
        )
        hio.to_file(init_file, lines)


def _test_encrypted_model() -> None:
    """
    Check that encrypted model works correctly.
    """
    temp_file_path = "./tmp.encrypt_model.test_encrypted_model.sh"
    hio.to_file(
        temp_file_path,
        'python -c "import dataflow_amp.pipelines.mock1_encrypted.mock1_pipeline as f; a = f.Mock1_DagBuilder(); print(a)"',
    )
    cmd = f"invoke docker_cmd -c 'bash {temp_file_path}'"
    (_, output) = hsystem.system_to_string(cmd)
    _LOG.info(output)
    os.remove(temp_file_path)


def _test_model() -> None:
    """
    Check that input model works correctly.
    """
    temp_file_path = "./tmp.encrypt_model.test_model.sh"
    hio.to_file(
        temp_file_path,
        'python -c "import dataflow_amp.pipelines.mock1.mock1_pipeline as f; a = f.Mock1_DagBuilder(); print(a)"',
    )
    cmd = f"invoke docker_cmd -c 'bash {temp_file_path}'"
    (_, output) = hsystem.system_to_string(cmd)
    _LOG.info(output)
    os.remove(temp_file_path)


# #############################################################################


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--source_dir",
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
    parser.add_argument("--test", required=False,
                        action="store_true",
                        help="Run testing")
    hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    #
    source_dir = args.source_dir
    hdbg.dassert_dir_exists(source_dir)
    target_dir = args.target_dir
    model_path = pathlib.Path(source_dir)
    model_name = model_path.stem
    if target_dir is not None:
        # Make sure user specified `target_dir` exists.
        hdbg.dassert_dir_exists(target_dir)
    else:
        # If no `target_dir` specified, save encrypted model in the same directory as the source model at.
        encrypted_model_name = "_".join([model_name, "encrypted"])
        target_dir = os.path.join(model_path.parent, encrypted_model_name)
        hio.create_dir(target_dir, incremental=True)
    _encrypt_model(source_dir, target_dir)
    if os.path.exists(os.path.join(target_dir, "__init__.py")):
        _tweak_init(target_dir)
    if args.test:
        _test_model()
        _test_encrypted_model()


if __name__ == "__main__":
    _main(_parse())
