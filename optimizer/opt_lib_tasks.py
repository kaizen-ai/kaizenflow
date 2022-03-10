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
    update_poetry=False,
):
    """
    Build a local image (i.e., a release candidate "dev" image).

    :param version: version to tag the image and code with
    :param cache: use the cache
    :param update_poetry: run poetry lock to update the packages
    """
    hlibtask._report_task()
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
def opt_docker_tag_local_image_as_dev(  # type: ignore
    ctx,
    version,
    base_image="",
):
    """
    (ONLY CI/CD) Mark the "local" image as "dev".

    :param version: version to tag the image and code with
    :param base_image: e.g., *****.dkr.ecr.us-east-1.amazonaws.com/amp
    """
    hlibtask._report_task()
    # TODO(Grisha): fix versioning.
    #version = _resolve_version_value(version)
    # Tag local image as versioned dev image (e.g., `dev-1.0.0`).
    image_versioned_local = hlibtask.get_image(base_image, "local", version)
    image_versioned_dev = hlibtask.get_image(base_image, "dev", version)
    cmd = f"docker tag {image_versioned_local} {image_versioned_dev}"
    hlibtask._run(ctx, cmd)
    # Tag local image as dev image.
    latest_version = None
    image_dev = hlibtask.get_image(base_image, "dev", latest_version)
    cmd = f"docker tag {image_versioned_local} {image_dev}"
    hlibtask._run(ctx, cmd)


@task
def opt_docker_push_dev_image(  # type: ignore
    ctx,
    version,
    base_image="",
):
    """
    (ONLY CI/CD) Push the "dev" image to ECR.

    :param version: version to tag the image and code with
    :param base_image: e.g., *****.dkr.ecr.us-east-1.amazonaws.com/amp
    """
    hlibtask._report_task()
    # TODO(Grisha): fix versioning.
    # version = hlibtask._resolve_version_value(version)
    #
    hlibtask.docker_login(ctx)
    # Push Docker versioned tag.
    image_versioned_dev = hlibtask.get_image(base_image, "dev", version)
    cmd = f"docker push {image_versioned_dev}"
    hlibtask._run(ctx, cmd, pty=True)
    # Push Docker tag.
    latest_version = None
    image_dev = hlibtask.get_image(base_image, "dev", latest_version)
    cmd = f"docker push {image_dev}"
    hlibtask._run(ctx, cmd, pty=True)


@task
def opt_docker_release_dev_image(  # type: ignore
    ctx,
    version,
    cache=True,
    push_to_repo=True,
    update_poetry=False,
):
    """
    (ONLY CI/CD) Build, test, and release to ECR the latest "dev" image.

    This can be used to test the entire flow from scratch by building an image,
    running the tests, but not necessarily pushing.

    Phases:
    1) Build local image
    2) Mark local as dev image
    3) Push dev image to the repo

    :param version: version to tag the image and code with
    :param cache: use the cache
    :param push_to_repo: push the image to the repo_short_name
    :param update_poetry: update package dependencies using poetry
    """
    hlibtask._report_task()
    # 1) Build "local" image.
    opt_docker_build_local_image(
        ctx,
        cache=cache,
        update_poetry=update_poetry,
        version=version,
    )
    # Run resolve after `docker_build_local_image` so that a proper check
    # for subsequent version can be made in case `FROM_CHANGELOG` token
    # is used.
    # TODO(Grisha): fix versioning.
    # version = _resolve_version_value(version)
    # TODO(Grisha): add `opt` tests.
    stage = "local"
    # 2) Promote the "local" image to "dev".
    opt_docker_tag_local_image_as_dev(ctx, version)
    # TODO(Grisha): add `qa` tests.
    # 3) Push the "dev" image to ECR.
    if push_to_repo:
        opt_docker_push_dev_image(ctx, version)
    else:
        _LOG.warning(
            "Skipping pushing dev image to repo_short_name, as requested"
        )
    _LOG.info("==> SUCCESS <==")


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
