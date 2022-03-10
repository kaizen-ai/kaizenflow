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

DOCKER_BUILDKIT = 0


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

    See more in `helpers/lib_tasks.py::docker_build_local_image`.
    """
    hlibtask.docker_build_local_image(
        ctx,
        version,
        cache=cache,
        base_image=base_image,
        update_poetry=update_poetry,
        dir_name="optimizer",
    )


@task
def opt_docker_tag_local_image_as_dev(  # type: ignore
    ctx,
    version,
    base_image="",
):
    """
    (ONLY CI/CD) Mark the `opt:local` image as `dev`.

    See more in `helpers/lib_tasks.py::docker_tag_local_image_as_dev`.
    """
    hlibtask.docker_tag_local_image_as_dev(
        ctx,
        version,
        base_image=base_image,
        dir_name="optimizer",
    )


@task
def opt_docker_push_dev_image(  # type: ignore
    ctx,
    version,
    base_image="",
):
    """
    (ONLY CI/CD) Push the `opt:dev` image to ECR.

    See more in `helpers/lib_tasks.py::docker_push_dev_image`.
    """
    hlibtask.docker_push_dev_image(
        ctx,
        version,
        base_image=base_image,
        dir_name="optimizer"
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

    See more in `helpers/lib_tasks.py::docker_release_dev_image`.

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
        dir_name="optimizer",
    )


@task
def opt_docker_bash(  # type: ignore
    ctx,
    stage="dev",
    version="",
    entrypoint=True,
    as_user=True,
):
    """
    Start a bash shell inside the `opt` container corresponding to a stage.

    See more in `helpers/lib_tasks.py::docker_bash`.
    """
    base_image = ""
    cmd = "bash"
    _docker_cmd = hlibtask._get_docker_cmd(
        base_image, stage, version, cmd, entrypoint=entrypoint, as_user=as_user
    )
    hlibtask._run(ctx, _docker_cmd, pty=True)


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

    See more in `helpers/lib_tasks.py::docker_jupyter`.
    """
    hlibtask._report_task()
    if auto_assign_port:
        uid = os.getuid()
        _LOG.debug("uid=%s", uid)
        git_repo_idx = hgit.get_project_dirname(only_index=True)
        git_repo_idx = int(git_repo_idx)
        _LOG.debug("git_repo_idx=%s", git_repo_idx)
        # We assume that there are no more than `max_idx_per_users` clients.
        max_idx_per_user = 10
        hdbg.dassert_lte(git_repo_idx, max_idx_per_user)
        port = (uid * max_idx_per_user) + git_repo_idx
        _LOG.info("Assigned port is %s", port)
    #
    print_docker_config = False
    docker_cmd_ = hlibtask._get_docker_jupyter_cmd(
        base_image,
        stage,
        version,
        port,
        self_test,
        print_docker_config=print_docker_config,
    )
    hlibtask._docker_cmd(ctx, docker_cmd_)
