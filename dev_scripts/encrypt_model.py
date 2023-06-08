#!/usr/bin/env python

"""
Encrypt model using pyarmor.

Usage:
> encrypt_model.py -d dir_name

Import as:

import dev_scripts.encrypt_model as dsenmo
"""

import argparse
import logging
import os
import pathlib
import tempfile

import helpers.hdbg as hdbg
import helpers.hparser as hparser
import helpers.hio as hio
import helpers.hsystem as hsystem

_LOG = logging.getLogger(__name__)


def _encrypt_model(dir_name: str, target_name: str) -> None:
    """
    Encrypt model using pyarmor.

    :param dir_name: model directory.
    :param target_name: encrypted model output directory.
    """
    # Create temporary Dockerfile.
    temp_file = tempfile.NamedTemporaryFile(suffix=".Dockerfile")
    temp_file.write(
        b"""
            FROM python:3.8
            RUN pip install pyarmor
        """)
    temp_file.flush()
    # Build Docker image.
    _LOG.info("Start building Docker image.")
    hsystem.system(f"docker build -f {temp_file.name} -t encryption_flow .")
    temp_file.close()
    # Run Docker container.
    work_dir = os.getcwd()
    hio.create_dir(target_name, incremental=False)
    mount = f"type=bind,source={work_dir},target={work_dir}"
    cmd = f"pyarmor-7 obfuscate --restrict=0 --recursive {dir_name} --output {target_name}"
    _LOG.info("Start running Docker container.")
    hsystem.system(f"docker run --rm -it --workdir {work_dir} --mount {mount} encryption_flow {cmd}")
    _LOG.info(f"Encrypted model successfully stored in {target_name}")
    _LOG.info("Remove temporary Dockerfile.")


def _tweak_init_py(target_name: str) -> None:
    """
    Import pyarmor_runtime into __init__.py file after encryption.

    :param target_name: encrypted model output directory.
    """
    _LOG.info("Start tweaking __init__.py.")
    init_file = os.path.join(target_name, "__init__.py")
    if os.path.exists(init_file):
        data = hio.from_file(init_file)
        hio.to_file(init_file, "from .pytransform import pyarmor_runtime; pyarmor_runtime()\n" + data)


def _test_encrypted_model():
    """
    Test if encrypted model works correctly.
    """
    temp = "./tmp.encrypt_model.test_encrypted_model.sh"
    hio.to_file(temp, 'python -c "import dataflow_amp.pipelines.mock1_encrypted.mock1_pipeline as f; a = f.Mock1_DagBuilder(); print(a)"')
    cmd = f"invoke docker_cmd -c 'bash {temp}'"
    (_, output) = hsystem.system_to_string(cmd)
    _LOG.info(output)
    os.remove(temp)


def _test_model():    
    """
    Test the model before encryption. 
    """
    temp = "./tmp.encrypt_model.test_model.sh"
    hio.to_file(temp, 'python -c "import dataflow_amp.pipelines.mock1.mock1_pipeline as f; a = f.Mock1_DagBuilder(); print(a)"')
    cmd = f"invoke docker_cmd -c 'bash {temp}'"
    (_, output) = hsystem.system_to_string(cmd)
    _LOG.info(output)
    os.remove(temp)


# #############################################################################


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "-d",
        "--dir_name",
        required=True,
        type=str,
        help="Model directory"
    )
    parser.add_argument(
        "-t",
        "--test",
        action="store_true",
        help="Run testing"
    )
    hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    #
    dir_name = args.dir_name
    hdbg.dassert_dir_exists(dir_name)
    model_path = pathlib.Path(dir_name)
    model_name = model_path.stem
    target_name = os.path.join(model_path.parent, model_name + "_encrypted")
    _encrypt_model(dir_name, target_name)
    if os.path.exists(os.path.join(target_name, "__init__.py")):
        _tweak_init_py(target_name)
    if args.test:
        _test_model()
        _test_encrypted_model()
    
    
if __name__ == "__main__":
    _main(_parse())
