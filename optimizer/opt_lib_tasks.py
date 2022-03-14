"""
Import as:

import optimizer.opt_lib_tasks as ooplitas
"""

import logging
import os

from invoke import task

import helpers.hdbg as hdbg
import helpers.hgit as hgit
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


def _get_docker_cmd(stage: str, command: str) -> str:
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
    cmd = ["docker-compose"]
    # Add `docker-compose` file path.
    docker_compose_file_path = hlibtask.get_base_docker_compose_path()
    cmd.append(f"--file {docker_compose_file_path}")
    # Add `env file` path.
    env_file = get_db_env_path(stage)
    cmd.append(f"--env-file {env_file}")
    # Add `run`.
    service_name = "opt_app"
    cmd.append(f"{command} --rm {service_name}")

    # Convert the list to a multiline command.
    multiline_docker_cmd = hlibtask._to_multi_line_cmd(cmd)
    return multiline_docker_cmd


@task
def opt_docker_up(ctx):
    command = "run"
    stage = "dev"
    docker_cmd = _get_docker_cmd(stage, command)

    # Execute the command.
    hlibtask._run(ctx, docker_cmd, pty=True)


@task
def opt_docker_down(ctx):
    command = "stop"
    stage = "dev"
    docker_cmd = _get_docker_cmd(stage, command)

    # Execute the command.
    hlibtask._run(ctx, docker_cmd, pty=True)