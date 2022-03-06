import logging
import os

from invoke import task

import helpers.hdbg as hdbg
import helpers.hgit as hgit
import helpers.hsystem as hsystem
import helpers.lib_tasks as hlibtask

_LOG = logging.getLogger(__name__)

DOCKER_BUILDKIT = 0


@task
def opt_docker_build_local_image(  # type: ignore
    ctx,
    version,
    cache=True,
    update_poetry=False,
):
    """
    Build a local image (i.e., a release candidate "dev" image).

    :param version: version to tag the image and code with
    :param cache: use the cache
    :param update_poetry: run poetry lock to update the packages
    """
    # _report_task()
    # TODO(gp): Enable the versioning.
    # _dassert_is_subsequent_version(version)
    # version = _resolve_version_value(version)
    # Update poetry, if needed.
    if update_poetry:
        cmd = "cd devops/docker_build; poetry lock -v"
        hlibtask._run(ctx, cmd)
    # Build the local image.
    opts = "--no-cache" if not cache else ""
    dockerfile = "devops/docker_build/dev.Dockerfile"
    dockerfile = os.path.abspath(dockerfile)
    base_image = ""
    stage = "local"
    image_local = hlibtask.get_image(base_image, stage, version)
    cmd = rf"""
    DOCKER_BUILDKIT={DOCKER_BUILDKIT} \
    time \
    docker build \
        --progress=plain \
        {opts} \
        --build-arg OPT_CONTAINER_VERSION={version} \
        --tag {image_local} \
        --file {dockerfile} \
        .
    """
    hlibtask._run(ctx, cmd)
    # Check image and report stats.
    cmd = f"docker image ls {image_local}"
    hlibtask._run(ctx, cmd)


@task
def opt_docker_bash(  # type: ignore
    ctx, stage, version, entrypoint=True, as_user=True
):
    base_image = ""
    cmd = "bash"
    _docker_cmd = hlibtask._get_docker_cmd(
        base_image, stage, version, cmd, entrypoint=entrypoint, as_user=as_user
    )
    hlibtask._run(ctx, _docker_cmd, pty=True)


@task
def opt_docker_jupyter(ctx, stage="local", version="0.1.0"):
    hlibtask._report_task()
    auto_assign_port = True
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
    base_image = ""
    port = 9999
    self_test = False
    docker_cmd_ = hlibtask._get_docker_jupyter_cmd(
        base_image,
        stage,
        version,
        port,
        self_test,
        print_docker_config=print_docker_config,
    )
    hlibtask._docker_cmd(ctx, docker_cmd_)
