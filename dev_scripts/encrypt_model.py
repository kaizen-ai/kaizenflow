#!/usr/bin/env python

"""
Encrypt model using pyarmor.

Usage:
> encrypt_model.py model_dir

Import as:

import dev_scripts.encrypt_model as dsenmo
"""

import argparse
import logging
import os.path as osp
from pathlib import Path

import helpers.hdbg as hdbg
import helpers.hparser as hparser

import helpers.hsystem as hsystem

_LOG = logging.getLogger(__name__)

# #############################################################################


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("model_dir", nargs=1, help="Model directory")
    hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    #
    model_dir = args.model_dir[0]
    if not osp.exists(model_dir):
        raise FileNotFoundError(f"Model directory: {model_dir} does not exist!")
    model_path = Path(model_dir)
    model_name = model_path.stem
    # Create temperary Dockerfile.
    tmp_filename = "/tmp/tmp.encrypt_model.Dockerfile"
    with open(tmp_filename, "w") as dockerfile:
        dockerfile.write(
            """
                FROM python:3.8
                RUN pip install pyarmor
            """
        )
    # Build Docker Image.
    _LOG.info("Start building Docker image.")
    hsystem.system_to_string(f"docker build -f {tmp_filename} -t encryption_flow .")
    _LOG.info("Finish building Docker image.")
    # Run Docker container
    work_dir = os.getcwd()
    target_dir = osp.join(model_path.parent, model_name + "_encrypted")
    os.makedirs(target_dir, exist_ok=True)
    mount = f"type=bind,source={work_dir},target={work_dir}"
    cmd = f"pyarmor-7 obfuscate --restrict=0 --recursive {model_dir} --output {target_dir}"
    _LOG.info("Start running Docker container.")
    hsystem.system_to_string(f"docker run --rm -it --workdir {work_dir} --mount {mount} encryption_flow {cmd}")
    _LOG.info(f"Encrypted model successfully stored in {target_dir}")
    # Add necessary import into __init__.py.
    init_dir = osp.join(target_dir, "__init__.py")
    if osp.exists(init_dir):
        with open(init_dir, "r") as original:
            data = original.read()
        with open(init_dir, "w") as modified:
            modified.write("from .pytransform import pyarmor_runtime; pyarmor_runtime()\n" + data)
    _LOG.info("Finished encryption flow.")
    # Remove temporary Dockerfile.
    os.remove(tmp_filename)
    _LOG.info("Remove temporary Dockerfile.")

if __name__ == "__main__":
    _main(_parse())
