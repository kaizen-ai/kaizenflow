#!/usr/bin/env python
"""
Encrypt a directory. The directory needs to contain all the required
dependencies, excluding the ones from packages present where the code will be
run (e.g., `helpers`).

Usage:
# Encrypt models with normal encryption flow.
> dev_scripts/encrypt_model.py \
    --model_dir dataflow_lemonade/pipelines \
    --model_dag_builder "C5a_DagBuilder" \
    --model_dag_builder_file "C5/C5a_pipeline.py" \
    -v DEBUG

# Encrypt models with cross-compile options.
> dev_scripts/encrypt_model.py \
    --model_dir dataflow_lemonade/pipelines \
    --build_target "linux/amd64" \
    --model_dag_builder "C5a_DagBuilder" \
    --model_dag_builder_file "C5/C5a_pipeline.py \
    -v DEBUG
"""
import argparse
import logging
import os
import pathlib

import helpers.hdbg as hdbg
import helpers.hio as hio
import helpers.hparser as hparser
import helpers.hsystem as hsystem

_LOG = logging.getLogger(__name__)


def _encrypt_model_dir(
    model_dir: str,
    target_dir: str,
    docker_build_target: str,
    docker_image_tag: str,
) -> str:
    """
    Encrypt a dir storing models using Pyarmor.

    :param model_dir: source model directory
    :param target_dir: encrypted model output directory
    :param build_target: Docker cross-building option (e.g. `linux/amd64`)
    :param docker_image_tag: Docker image tag
    :return: encrypted model directory
    """
    hdbg.dassert_type_is(docker_image_tag, str)
    # Create encrypted model directory.
    model_path = pathlib.Path(model_dir)
    model_name = model_path.stem
    encrypted_model_name = "_".join(["encrypted", model_name])
    encrypted_model_dir = os.path.join(target_dir, encrypted_model_name)
    hio.create_dir(encrypted_model_dir, incremental=False)
    # Get user and group id.
    user_id_cmd = "id -u"
    _, user_id = hsystem.system_to_string(user_id_cmd)
    group_id_cmd = "id -g"
    _, group_id = hsystem.system_to_string(group_id_cmd)
    # Create temporary Dockerfile.
    temp_dockerfile_path = "./tmp.encrypt_model.Dockerfile"
    with open(temp_dockerfile_path, "w") as temp_dockerfile:
        temp_dockerfile.write(
            f"""
                FROM python:3.8
                RUN groupadd -g {group_id} mygroup && useradd -u {user_id} -g mygroup myuser
                RUN pip install pyarmor
                RUN mkdir -p /home/myuser && chown myuser:mygroup /home/myuser
                USER myuser
            """
        )
    # Build Docker image.
    _LOG.info("Building Docker image from %s", temp_dockerfile_path)
    if docker_build_target is not None:
        cmd = f"docker buildx build --platform {docker_build_target} -f {temp_dockerfile_path} -t {docker_image_tag} --build-arg user_id={user_id} --build-arg group_id={group_id} ."
    else:
        cmd = f"docker build -f {temp_dockerfile.name} -t {docker_image_tag} --build-arg user_id={user_id} --build-arg group_id={group_id} ."
    (_, output) = hsystem.system_to_string(cmd)
    _LOG.debug(output)
    os.remove(temp_dockerfile_path)
    # Run Docker container to encrypt the model.
    _LOG.info("Running Docker container.")
    work_dir = os.getcwd()
    docker_target_dir = "/app"
    mount = f"type=bind,source={work_dir},target={docker_target_dir}"
    encryption_flow = f"pyarmor-7 obfuscate --restrict=0 --recursive {model_dir} --output {encrypted_model_dir}"
    if docker_build_target is not None:
        docker_cmd = f"docker run --rm -it --user {user_id}:{group_id} --platform {docker_build_target} --workdir {docker_target_dir} --mount {mount} {docker_image_tag} {encryption_flow}"
    else:
        docker_cmd = f"docker run --rm -it --user {user_id}:{group_id} --workdir {docker_target_dir} --mount {mount} {docker_image_tag} {encryption_flow}"
    (_, output) = hsystem.system_to_string(docker_cmd)
    _LOG.debug(output)
    # Check that command worked by ensuring that there are dirs in the target dir.
    n_files = len(os.listdir(encrypted_model_dir))
    hdbg.dassert_lt(
        0, n_files, "No files in encrypted_model_dir=`%s`", encrypted_model_dir
    )
    _LOG.info(
        "Encrypted model successfully stored in encrypted_model_dir='%s'",
        encrypted_model_dir,
    )
    return encrypted_model_dir


def _tweak_init(encrypted_model_dir: str) -> None:
    """
    Add Pyarmor module to `__init__.py` to make sure that encrypted model works
    correctly.

    :param encrypted_model_dir: encrypted model directory (e.g.
        dataflow_amp/encrypted_pipelines)
    """
    # Generate absolute path of `pytransform` import.
    encrypted_model_import_path = encrypted_model_dir.strip("/")
    encrypted_model_import_path = encrypted_model_import_path.replace("/", ".")
    pytransform_import_path = ".".join(
        [encrypted_model_import_path, "pytransform"]
    )
    pytransform_import = f"from {pytransform_import_path} import pyarmor_runtime; pyarmor_runtime()"
    # Find all `__init__.py` under encrypted model directory except for the one
    # under `pytransform`.
    cmd = f'find {encrypted_model_dir} -name "__init__.py"'
    _, init_files = hsystem.system_to_string(cmd)
    files = init_files.split("\n")
    for f in files:
        file_path = pathlib.Path(f)
        if file_path.parent.name != "pytransform":
            data = hio.from_file(f)
            lines = "\n".join([pytransform_import, data])
            hio.to_file(f, lines)


def _test_model(model_dag_builder: str, model_dag_builder_file: str) -> None:
    """
    Check that a model works correctly.

    :param model_dag_builder: e.g., C5a_DagBuilder
    :param model_dag_builder_file: e.g.,
        dataflow_lemonade/pipelines/C5/C5a_pipeline.py
    """
    # _LOG.debug(hprint.to_str("model_dir model_dag_builder model_dag_builder_file"))
    # The expected Python command is:
    #   ```
    #   import dataflow_lemonade.pipelines.C5.C5a_pipeline as f;
    #   a = f.C5a_DagBuilder(); print(a)
    #   ```
    import_path = (
        pathlib.Path(model_dag_builder_file)
        .with_suffix("")
        .as_posix()
        .replace("/", ".")
    )
    script = f'python -c "import {import_path} as f; a = f.{model_dag_builder}(); print(a)"'
    # Write testing script to temporary file.
    temp_file_path = "./tmp.encrypt_model.test_model.sh"
    hio.to_file(temp_file_path, script)
    # Run test inside Docker container.
    cmd = f"invoke docker_cmd -c 'bash {temp_file_path}'"
    _, output = hsystem.system_to_string(cmd)
    _LOG.debug(output)
    os.remove(temp_file_path)


def _test_models_in_dir(model_dir: str, model_dag_builder: str) -> None:
    """
    Test all models in a directory.
    """
    for model in os.scandir(model_dir):
        if model.is_dir() and model.name != "pytransform":
            _LOG.info("Testing model %s.", model.name)
            _test_model(model.path, model_dag_builder)


# #############################################################################


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--model_dir", required=True, type=str, help="Source model directory"
    )
    parser.add_argument(
        "--model_dag_builder",
        required=False,
        type=str,
        help="Model dag builder name",
    )
    parser.add_argument(
        "--model_dag_builder_file",
        required=False,
        type=str,
        help="Path to the file storing the model dag builder",
    )
    parser.add_argument(
        "--build_target",
        default=None,
        choices=["linux/arm64", "linux/amd64", "linux/amd64,linux/arm64"],
        type=str,
        help="Specify cross-build options for docker container",
    )
    parser.add_argument(
        "--target_dir",
        required=False,
        default=None,
        type=str,
        help="Encrypted model output directory",
    )
    parser.add_argument(
        "--release_dir",
        required=False,
        default=None,
        type=str,
        help="Encrypted model output directory",
    )

    parser.add_argument(
        "--docker_image_tag",
        required=False,
        default="encryption_flow",
        type=str,
        help="Docker image tag",
    )
    parser.add_argument(
        "--test", default=True, action="store_true", help="Run testing"
    )
    hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    #
    model_dir = args.model_dir
    hdbg.dassert_dir_exists(model_dir)
    hdbg.dassert_file_exists(os.path.join(model_dir, args.model_dag_builder_file))
    target_dir = args.target_dir
    # If `target_dir` is not specified, use the parent directory of `model_dir`.
    if target_dir is None:
        model_path = pathlib.Path(model_dir)
        target_dir = str(model_path.parent)
    # Get the DAG builder.
    if args.test:
        hdbg.dassert_is_not(args.model_dag_builder, None)
    if args.model_dag_builder is not None:
        hdbg.dassert_is_not(args.model_dag_builder_file, None)
    if args.test:
        _LOG.info("Testing original model.")
        model_dag_builder_file_org = os.path.join(
            model_dir, args.model_dag_builder_file
        )
        _test_model(args.model_dag_builder, model_dag_builder_file_org)
    # Encrypt the dir.
    encrypted_model_dir = _encrypt_model_dir(
        model_dir, target_dir, args.build_target, args.docker_image_tag
    )
    # Tweak `__init__.py` file.
    _tweak_init(encrypted_model_dir)
    #
    if args.test:
        _LOG.info("Testing encrypted model.")
        model_dag_builder_file_enc = os.path.join(
            encrypted_model_dir, args.model_dag_builder_file
        )
        _test_model(args.model_dag_builder, model_dag_builder_file_enc)

    if args.release_dir:
        # Extract the git root in release folder.
        # Copy the encrypted files to the release dir and add them to Git, removing the old files.
        hio.create_dir(args.release_dir, incremental=True)
        _, release_git_root = hsystem.system_to_string(
            f"cd {args.release_dir}; git rev-parse --show-toplevel"
        )
        release_base_dir = os.path.relpath(args.release_dir, release_git_root)
        _, output = hsystem.system_to_string(
            f"cd {release_git_root}; git rm -rf --ignore-unmatch {release_base_dir}"
        )
        encrypted_model_dir = os.path.abspath(encrypted_model_dir)
        _, output = hsystem.system_to_string(
            f"cp -r {encrypted_model_dir}/* {args.release_dir}"
        )
        _, output = hsystem.system_to_string(
            f"cd {release_git_root}; git add {release_base_dir}"
        )


if __name__ == "__main__":
    _main(_parse())
