"""
Import as:

import optimizer.opt_lib_tasks as ooplitas
"""

import logging
import os
import re
from typing import List, Any, Optional

from invoke import task

import helpers.hdbg as hdbg
import helpers.hgit as hgit
import helpers.hsystem as hsystem
import helpers.lib_tasks as hlibtask

_LOG = logging.getLogger(__name__)


_OPTIMIZER_DIR = os.path.join(hgit.get_amp_abs_path(), "optimizer")


# #############################################################################
# Docker image release.
# #############################################################################


@task
def opt_docker_build_local_image(  # type: ignore
    ctx,
    version,
    cache=True,
    base_image="",
    update_poetry=False,
):
    """
    Build a local `opt` image (i.e., a release candidate "dev" image).

    See corresponding invoke target for the main container.
    """
    hlibtask.docker_build_local_image(
        ctx,
        version,
        cache=cache,
        base_image=base_image,
        update_poetry=update_poetry,
        container_dir_name=_OPTIMIZER_DIR,
    )


@task
def opt_docker_tag_local_image_as_dev(  # type: ignore
    ctx,
    version,
    base_image="",
):
    """
    (ONLY CI/CD) Mark the `opt:local` image as `dev`.

    See corresponding invoke target for the main container.
    """
    hlibtask.docker_tag_local_image_as_dev(
        ctx,
        version,
        base_image=base_image,
        container_dir_name=_OPTIMIZER_DIR,
    )


@task
def opt_docker_push_dev_image(  # type: ignore
    ctx,
    version,
    base_image="",
):
    """
    (ONLY CI/CD) Push the `opt:dev` image to ECR.

    See corresponding invoke target for the main container.
    """
    hlibtask.docker_push_dev_image(
        ctx, version, base_image=base_image, container_dir_name=_OPTIMIZER_DIR
    )


@task
def opt_docker_release_dev_image(  # type: ignore
    ctx,
    version,
    cache=True,
    push_to_repo=True,
    update_poetry=False,
):
    """
    (ONLY CI/CD) Build, test, and release to ECR the latest `opt:dev` image.

    See corresponding invoke target for the main container.

    Phases:
    1) Build local image
    2) Mark local as dev image
    3) Push dev image to the repo
    """
    hlibtask.docker_release_dev_image(
        ctx,
        version,
        cache=cache,
        # TODO(Grisha): replace with `opt` tests.
        skip_tests=True,
        fast_tests=False,
        slow_tests=False,
        superslow_tests=False,
        # TODO(Grisha): enable `qa` tests.
        qa_tests=False,
        push_to_repo=push_to_repo,
        update_poetry=update_poetry,
        container_dir_name=_OPTIMIZER_DIR,
    )


# #############################################################################
# Docker development.
# #############################################################################


@task
def opt_docker_bash(  # type: ignore
    ctx,
    base_image="",
    stage="dev",
    version="",
    entrypoint=True,
    as_user=True,
):
    """
    Start a bash shell inside the `opt` container corresponding to a stage.

    See corresponding invoke target for the main container.
    """
    hlibtask.docker_bash(
        ctx,
        base_image=base_image,
        stage=stage,
        version=version,
        entrypoint=entrypoint,
        as_user=as_user,
        container_dir_name=_OPTIMIZER_DIR,
    )


@task
def opt_docker_jupyter(  # type: ignore
    ctx,
    stage="dev",
    version="",
    base_image="",
    auto_assign_port=True,
    port=9999,
    self_test=False,
):
    """
    Run jupyter notebook server in the `opt` container.

    See corresponding invoke target for the main container.
    """
    hlibtask.docker_jupyter(
        ctx,
        stage=stage,
        version=version,
        base_image=base_image,
        auto_assign_port=auto_assign_port,
        port=port,
        self_test=self_test,
        container_dir_name=_OPTIMIZER_DIR,
    )


# #############################################################################
# Docker up / down.
# #############################################################################
_DEFAULT_PARAMS = {}
_INTERNET_ADDRESS_RE = r"([a-z0-9]+(-[a-z0-9]+)*\.)+[a-z]{2,}"
_IMAGE_BASE_NAME_RE = r"[a-z0-9_-]+"
_IMAGE_VERSION_RE = r"\d+\.\d+\.\d+"
_IMAGE_USER_RE = r"[a-z0-9_-]+"
# For candidate prod images which have added hash for easy identification.
_IMAGE_HASH_RE = r"[a-z0-9]{9}"
_IMAGE_STAGE_RE = (
    rf"(local(?:-{_IMAGE_USER_RE})?|dev|prod|prod(?:-{_IMAGE_HASH_RE})?)"
)


def get_default_param(key: str) -> Any:
    hdbg.dassert_in(key, _DEFAULT_PARAMS)
    hdbg.dassert_isinstance(key, str)
    return _DEFAULT_PARAMS[key]


def get_db_env_path(stage: str) -> str:
    """
    Get path to db env file that contains db connection parameters.
    :param stage: development stage, i.e. `local`, `dev` and `prod`
    """
    hdbg.dassert_in(stage, "local dev prod".split())
    # Get `env` files dir.
    env_dir = "im_v2/devops/env"
    # Get the file name depending on the stage.
    env_file_name = f"{stage}.im_db_config.env"
    # Get file path.
    amp_path = hgit.get_amp_abs_path()
    env_file_path = os.path.join(amp_path, env_dir, env_file_name)
    hdbg.dassert_file_exists(env_file_path)
    return env_file_path


def _dassert_is_version_valid(version: str) -> None:
    """
    A valid version looks like: `1.0.0`.
    """
    hdbg.dassert_isinstance(version, str)
    hdbg.dassert_ne(version, "")
    regex = rf"^({_IMAGE_VERSION_RE})$"
    _LOG.debug("Testing with regex='%s'", regex)
    m = re.match(regex, version)
    hdbg.dassert(m, "Invalid version: '%s'", version)


def _dassert_is_base_image_name_valid(base_image: str) -> None:
    """
    A base image should look like.
    *****.dkr.ecr.us-east-1.amazonaws.com/amp
    """
    regex = rf"^{_INTERNET_ADDRESS_RE}\/{_IMAGE_BASE_NAME_RE}$"
    _LOG.debug("regex=%s", regex)
    m = re.match(regex, base_image)
    hdbg.dassert(m, "Invalid base_image: '%s'", base_image)


def _dassert_is_image_name_valid(image: str) -> None:
    """
    Check whether an image name is valid.
    Invariants:
    - Local images contain a user name and a version
      - E.g., `*****.dkr.ecr.us-east-1.amazonaws.com/amp:local-saggese-1.0.0`
    - `dev` and `prod` images have an instance with the a version and one without
      to indicate the latest
      - E.g., `*****.dkr.ecr.us-east-1.amazonaws.com/amp:dev-1.0.0`
        and `*****.dkr.ecr.us-east-1.amazonaws.com/amp:dev`
    - `prod` candidate image has a 9 character hash identifier from the
        corresponding Git commit
        - E.g., `*****.dkr.ecr.us-east-1.amazonaws.com/amp:prod-1.0.0-4rf74b83a`
    An image should look like:
    *****.dkr.ecr.us-east-1.amazonaws.com/amp:dev
    *****.dkr.ecr.us-east-1.amazonaws.com/amp:local-saggese-1.0.0
    *****.dkr.ecr.us-east-1.amazonaws.com/amp:dev-1.0.0
    """
    regex = "".join(
        [
            # E.g., *****.dkr.ecr.us-east-1.amazonaws.com/amp
            rf"^{_INTERNET_ADDRESS_RE}\/{_IMAGE_BASE_NAME_RE}",
            # :local-saggese
            rf":{_IMAGE_STAGE_RE}",
            # -1.0.0
            rf"(-{_IMAGE_VERSION_RE})?$",
        ]
    )
    _LOG.debug("Testing with regex='%s'", regex)
    m = re.match(regex, image)
    hdbg.dassert(m, "Invalid image: '%s'", image)


def _get_base_image(base_image: str) -> str:
    """
    :return: e.g., *****.dkr.ecr.us-east-1.amazonaws.com/amp
    """
    if base_image == "":
        # TODO(gp): Use os.path.join.
        base_image = (
            get_default_param("ECR_BASE_PATH")
            + "/"
            + get_default_param("BASE_IMAGE")
        )
    _dassert_is_base_image_name_valid(base_image)
    return base_image


def get_image(
    base_image: str,
    stage: str,
    version: Optional[str],
) -> str:
    """
    Return the fully qualified image name.
    For local stage, it also appends the user name to the image name.
    :param base_image: e.g., *****.dkr.ecr.us-east-1.amazonaws.com/amp
    :param stage: e.g., `local`, `dev`, `prod`
    :param version: e.g., `1.0.0`, if None empty, the latest version is used
    :return: e.g., `*****.dkr.ecr.us-east-1.amazonaws.com/amp:local` or
        `*****.dkr.ecr.us-east-1.amazonaws.com/amp:local-1.0.0`
    """
    # Docker refers the default image as "latest", although in our stage
    # nomenclature we call it "dev".
    hdbg.dassert_in(stage, "local dev prod".split())
    # Get the base image.
    base_image = _get_base_image(base_image)
    _dassert_is_base_image_name_valid(base_image)
    # Get the full image name.
    image = [base_image]
    # Handle the stage.
    image.append(f":{stage}")
    # User the user name.
    if stage == "local":
        user = hsystem.get_user_name()
        image.append(f"-{user}")
    # Handle the version.
    if version is not None and version != "":
        _dassert_is_version_valid(version)
        image.append(f"-{version}")
    #
    image = "".join(image)
    _dassert_is_image_name_valid(image)
    return image


def _get_docker_cmd(
    command: str,
    stage="dev",
    version=None,
    base_image="665840871993.dkr.ecr.us-east-1.amazonaws.com/cmamp"
) -> str:
    """
    Construct the `docker-compose' command to run a script inside this
    container Docker component.
    E.g, to run the `.../devops/set_schema_im_db.py`:
    ```
    docker-compose \
        --file devops/compose/docker-compose.yml \
        --env-file devops/env/local.im_db_config.env \
        run --rm im_postgres \
        .../devops/set_schema_im_db.py
    ```
    :param stage: development stage, i.e. `local`, `dev` and `prod`
    :param docker_cmd: command to execute inside docker
    """

    cmd: List[str] = []
    extra_env_vars: Optional[List[str]] = ["PORT=9999"]

    # - Handle the image.
    image = get_image(base_image, stage, version)
    _LOG.debug("base_image=%s stage=%s -> image=%s", base_image, stage, image)
    _dassert_is_image_name_valid(image)
    cmd.append(f"IMAGE={image}")

    # - Handle extra env vars.
    if extra_env_vars:
        hdbg.dassert_isinstance(extra_env_vars, list)
        for env_var in extra_env_vars:
            cmd.append(f"{env_var}")
    #

    cmd = ["docker-compose"]

    # Add `docker-compose` file path.
    docker_compose_file_path = hlibtask.get_base_docker_compose_path()
    cmd.append(f"--file {docker_compose_file_path}")

    # Add `env file` path.
    # - Handle the env file.
    env_file = "devops/env/default.env"
    cmd.append(
        rf"""
            --env-file {env_file}"""
    )

    # Add `run`.
    service_name = "opt_app"
    cmd.append(f"{command} --rm {service_name}")

    # Convert the list to a multiline command.
    multiline_docker_cmd = hlibtask._to_multi_line_cmd(cmd)
    return multiline_docker_cmd


@task
def opt_docker_up(ctx):
    command = "run"
    docker_cmd = _get_docker_cmd(command)

    # Execute the command.
    hlibtask._run(ctx, docker_cmd, pty=True)


@task
def opt_docker_down(ctx):
    command = "stop"
    docker_cmd = _get_docker_cmd(command)

    # Execute the command.
    hlibtask._run(ctx, docker_cmd, pty=True)