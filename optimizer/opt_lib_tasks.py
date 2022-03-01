import logging
import os

from invoke import task

import helpers.hsystem as hsystem
import helpers.lib_tasks as hlib
import helpers.hgit as hgit
import helpers.hdbg as hdbg


_LOG = logging.getLogger(__name__)
DOCKER_BUILDKIT = 0


@task
def docker_build_local_opt_image(ctx, version):
    """
    ...

    :param version:
    :return:
    """
    docker_file = "devops/docker_build/dev.Dockerfile"
    abs_docker_file = os.path.abspath(docker_file)
    base_image = ""
    stage = "local"
    image_local = hlib.get_image(base_image, stage, version)
    cmd = rf"""
    DOCKER_BUILDKIT={DOCKER_BUILDKIT} \
    time \
    docker build \
        --progress=plain \
        --build-arg OPT_CONTAINER_VERSION={version} \
        --tag {image_local} \
        --file {abs_docker_file} \
        .
    """
    hlib._run(ctx, cmd)
    #
    cmd = f"docker image ls {image_local}"
    hlib._run(ctx, cmd)


@task
def opt_docker_bash(ctx, stage, version):
    base_image = ""
    cmd = "bash"
    # TODO(Grisha): turn on `as_user`.
    _docker_cmd = hlib._get_docker_cmd(base_image, stage, version, cmd, entrypoint=True, as_user=False)
    hlib._run(ctx, _docker_cmd, pty=True)


@task
def opt_docker_jupyter(ctx, stage="local", version="0.1.0"):
    hlib._report_task()
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
    docker_cmd_ = hlib._get_docker_jupyter_cmd(
        base_image,
        stage,
        version,
        port,
        self_test,
        print_docker_config=print_docker_config,
    )
    hlib._docker_cmd(ctx, docker_cmd_)