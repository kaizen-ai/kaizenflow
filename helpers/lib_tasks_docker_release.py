"""
Import as:

import helpers.lib_tasks_docker_release as hltadore
"""

import logging
import os
from operator import attrgetter
from typing import Any

from invoke import task

# We want to minimize the dependencies from non-standard Python packages since
# this code needs to run with minimal dependencies and without Docker.
import helpers.hdbg as hdbg
import helpers.hgit as hgit
import helpers.hs3 as hs3
import helpers.lib_tasks_docker as hlitadoc
import helpers.lib_tasks_pytest as hlitapyt
import helpers.lib_tasks_utils as hlitauti

_LOG = logging.getLogger(__name__)

# pylint: disable=protected-access


# #############################################################################
# Docker image workflows.
# #############################################################################


def _to_abs_path(filename: str) -> str:
    filename = os.path.abspath(filename)
    hdbg.dassert_path_exists(filename)
    return filename


def _prepare_docker_ignore(ctx: Any, docker_ignore: str) -> None:
    """
    Copy the target docker_ignore in the proper position for `docker build`.
    """
    # Currently there is no built-in way to control which .dockerignore to use.
    # https://stackoverflow.com/questions/40904409
    hdbg.dassert_path_exists(docker_ignore)
    cmd = f"cp -f {docker_ignore} .dockerignore"
    hlitauti.run(ctx, cmd)


# =============================================================================
# DEV image flow
# =============================================================================
# - A "local" image (which is a release candidate for the DEV image) is built with:
#   ```
#   > docker_build_local_image
#   ```
#   This creates the local image `dev_tools:local.saggese-1.0.0`
# - A qualification process (e.g., running all unit tests and the QA tests) is
#   performed on the "local" image (e.g., locally or through GitHub actions)
# - If the qualification process is passed, the image is released as `dev` on ECR


# Use Docker buildkit or not.
# DOCKER_BUILDKIT = 1
DOCKER_BUILDKIT = 0


# For base_image, we use "" as default instead None since pyinvoke can only infer
# a single type.
@task
def docker_build_local_image(  # type: ignore
    ctx,
    version,
    cache=False,
    base_image="",
    update_poetry=False,
    container_dir_name=".",
    just_do_it=False,
):
    """
    Build a local image (i.e., a release candidate "dev" image).

    :param version: version to tag the image and code with
    :param cache: use the cache
    :param base_image: e.g., *****.dkr.ecr.us-east-1.amazonaws.com/amp
    :param update_poetry: run poetry lock to update the packages
    :param just_do_it: execute the action ignoring the checks
    """
    hlitauti.report_task(container_dir_name=container_dir_name)
    if just_do_it:
        _LOG.warning("Skipping subsequent version check")
    else:
        hlitadoc._dassert_is_subsequent_version(
            version, container_dir_name=container_dir_name
        )
    prod_version = hlitadoc._resolve_version_value(
        version, container_dir_name=container_dir_name
    )
    dev_version = hlitadoc._to_dev_version(prod_version)
    # Update poetry, if needed.
    if update_poetry:
        cmd = "cd devops/docker_build; poetry lock -v"
        hlitauti.run(ctx, cmd)
    # Prepare `.dockerignore`.
    docker_ignore = ".dockerignore.dev"
    _prepare_docker_ignore(ctx, docker_ignore)
    # Build the local image.
    image_local = hlitadoc.get_image(base_image, "local", dev_version)
    hlitadoc._dassert_is_image_name_valid(image_local)
    # This code path through Git tag was discontinued with CmTask746.
    # git_tag_prefix = get_default_param("BASE_IMAGE")
    # container_version = get_git_tag(version)
    #
    # TODO: Use the same approach as sorrentum_sandbox/docker_common/utils.sh
    # We want to enable using multiarch and build for both a single arch or more.
    PLATFORM_BUILDER = "sorrentumBuilder"
    cmd = rf"""
    docker buildx create --name {PLATFORM_BUILDER} --driver docker-container --bootstrap && docker buildx use {PLATFORM_BUILDER}
    """
    hlitauti.run(ctx, cmd)
    #
    dockerfile = "devops/docker_build/dev.Dockerfile"
    dockerfile = _to_abs_path(dockerfile)
    #
    opts = "--no-cache" if not cache else ""
    # TODO(gp): Use _to_multi_line_cmd()
    cmd = rf"""
    DOCKER_BUILDKIT={DOCKER_BUILDKIT} \
    time \
    docker buildx build \
        {opts} \
        --platform linux/amd64,linux/arm64 \
        --build-arg AM_CONTAINER_VERSION={dev_version} \
        --tag {image_local} \
        --file {dockerfile} \
        .
    """
    hlitauti.run(ctx, cmd)
    
    cmd = rf"""
    docker buildx rm {PLATFORM_BUILDER}
    """
    hlitauti.run(ctx, cmd)   
    
    # Check image and report stats.
    cmd = f"docker image ls {image_local}"
    hlitauti.run(ctx, cmd)

@task
def docker_tag_local_image_as_dev(  # type: ignore
    ctx,
    version,
    base_image="",
    container_dir_name=".",
):
    """
    (ONLY CI/CD) Mark the "local" image as "dev".

    :param version: version to tag the image and code with
    :param base_image: e.g., *****.dkr.ecr.us-east-1.amazonaws.com/amp
    """
    hlitauti.report_task(container_dir_name=container_dir_name)
    prod_version = hlitadoc._resolve_version_value(
        version, container_dir_name=container_dir_name
    )
    dev_version = hlitadoc._to_dev_version(prod_version)
    # Tag local image as versioned dev image (e.g., `dev-1.0.0`).
    image_versioned_local = hlitadoc.get_image(base_image, "local", dev_version)
    image_versioned_dev = hlitadoc.get_image(base_image, "dev", dev_version)
    cmd = f"docker tag {image_versioned_local} {image_versioned_dev}"
    hlitauti.run(ctx, cmd)
    # Tag local image as dev image.
    latest_version = None
    image_dev = hlitadoc.get_image(base_image, "dev", latest_version)
    cmd = f"docker tag {image_versioned_local} {image_dev}"
    hlitauti.run(ctx, cmd)


@task
def docker_push_dev_image(  # type: ignore
    ctx,
    version,
    base_image="",
    container_dir_name=".",
):
    """
    (ONLY CI/CD) Push the "dev" image to ECR.

    :param version: version to tag the image and code with
    :param base_image: e.g., *****.dkr.ecr.us-east-1.amazonaws.com/amp
    """
    hlitauti.report_task(container_dir_name=container_dir_name)
    prod_version = hlitadoc._resolve_version_value(
        version, container_dir_name=container_dir_name
    )
    dev_version = hlitadoc._to_dev_version(prod_version)
    #
    hlitadoc.docker_login(ctx)
    # Push Docker versioned tag.
    image_versioned_dev = hlitadoc.get_image(base_image, "dev", dev_version)
    cmd = f"docker push {image_versioned_dev}"
    hlitauti.run(ctx, cmd, pty=True)
    # Push Docker tag.
    latest_version = None
    image_dev = hlitadoc.get_image(base_image, "dev", latest_version)
    cmd = f"docker push {image_dev}"
    hlitauti.run(ctx, cmd, pty=True)


@task
def docker_release_dev_image(  # type: ignore
    ctx,
    version,
    cache=True,
    skip_tests=False,
    fast_tests=True,
    slow_tests=True,
    superslow_tests=False,
    qa_tests=True,
    push_to_repo=True,
    update_poetry=True,
    container_dir_name=".",
):
    """
    (ONLY CI/CD) Build, test, and release to ECR the latest "dev" image.

    This can be used to test the entire flow from scratch by building an image,
    running the tests, but not necessarily pushing.

    Phases:
    1) Build local image
    2) Run the unit tests (e.g., fast, slow, superslow) on the local image
    3) Mark local as dev image
    4) Run the QA tests on the dev image
    5) Push dev image to the repo

    :param version: version to tag the image and code with
    :param cache: use the cache
    :param skip_tests: skip all the tests and release the dev image
    :param fast_tests: run fast tests, unless all tests skipped
    :param slow_tests: run slow tests, unless all tests skipped
    :param superslow_tests: run superslow tests, unless all tests skipped
    :param qa_tests: run end-to-end linter tests, unless all tests skipped
    :param push_to_repo: push the image to the repo_short_name
    :param update_poetry: update package dependencies using poetry
    """
    hlitauti.report_task(container_dir_name=container_dir_name)
    # 1) Build "local" image.
    docker_build_local_image(
        ctx,
        cache=cache,
        update_poetry=update_poetry,
        version=version,
        container_dir_name=container_dir_name,
    )
    # Run resolve after `docker_build_local_image` so that a proper check
    # for subsequent version can be made in case `FROM_CHANGELOG` token
    # is used.
    prod_version = hlitadoc._resolve_version_value(
        version, container_dir_name=container_dir_name
    )
    dev_version = hlitadoc._to_dev_version(prod_version)
    # 2) Run tests for the "local" image.
    if skip_tests:
        _LOG.warning("Skipping all tests and releasing")
        fast_tests = False
        slow_tests = False
        superslow_tests = False
        qa_tests = False
    stage = "local"
    if fast_tests:
        hlitapyt.run_fast_tests(ctx, stage=stage, version=dev_version)
    if slow_tests:
        hlitapyt.run_slow_tests(ctx, stage=stage, version=dev_version)
    if superslow_tests:
        hlitapyt.run_superslow_tests(ctx, stage=stage, version=dev_version)
    # 3) Promote the "local" image to "dev".
    docker_tag_local_image_as_dev(
        ctx, dev_version, container_dir_name=container_dir_name
    )
    # 4) Run QA tests for the (local version) of the dev image.
    if qa_tests:
        hlitapyt.run_qa_tests(ctx, stage="dev", version=dev_version)
    # 5) Push the "dev" image to ECR.
    if push_to_repo:
        docker_push_dev_image(
            ctx, dev_version, container_dir_name=container_dir_name
        )
    else:
        _LOG.warning(
            "Skipping pushing dev image to repo_short_name, as requested"
        )
    _LOG.info("==> SUCCESS <==")


# #############################################################################
# PROD image flow:
# #############################################################################
# - PROD image has no release candidate
# - Start from a DEV image already built and qualified
# - The PROD image is created from the DEV image by copying the code inside the
#   image
# - The PROD image is tagged as "prod"


# TODO(gp): Remove redundancy with docker_build_local_image(), if possible.
@task
def docker_build_prod_image(  # type: ignore
    ctx,
    version,
    cache=True,
    base_image="",
    candidate=False,
    user_tag="",
    container_dir_name=".",
):
    """
    (ONLY CI/CD) Build a prod image.

    Phases:
    - Build the prod image on top of the dev image

    :param version: version to tag the image and code with
    :param cache: note that often the prod image is just a copy of the dev
        image so caching makes no difference
    :param base_image: e.g., *****.dkr.ecr.us-east-1.amazonaws.com/amp
    :param candidate: build a prod image with a tag format: prod-{hash}
        where hash is the output of hgit.get_head_hash
    :param user_tag: the name of the user building the candidate image
    """
    hlitauti.report_task(container_dir_name=container_dir_name)
    prod_version = hlitadoc._resolve_version_value(
        version, container_dir_name=container_dir_name
    )
    # Prepare `.dockerignore`.
    docker_ignore = ".dockerignore.prod"
    _prepare_docker_ignore(ctx, docker_ignore)
    # TODO(gp): We should do a `i git_clean` to remove artifacts and check that
    #  the client is clean so that we don't release from a dirty client.
    # Build prod image.
    if candidate:
        # For candidate prod images which need to be tested on
        # the AWS infra add a hash identifier.
        latest_version = None
        image_versioned_prod = hlitadoc.get_image(
            base_image, "prod", latest_version
        )
        head_hash = hgit.get_head_hash(short_hash=True)
        # Add user name to the prod image name.
        if user_tag:
            image_versioned_prod += f"-{user_tag}"
        # Add head hash to the prod image name.
        image_versioned_prod += f"-{head_hash}"
    else:
        image_versioned_prod = hlitadoc.get_image(
            base_image, "prod", prod_version
        )
    hlitadoc._dassert_is_image_name_valid(image_versioned_prod)
    #
    dockerfile = "devops/docker_build/prod.Dockerfile"
    dockerfile = _to_abs_path(dockerfile)
    #
    # TODO(gp): Use _to_multi_line_cmd()
    opts = "--no-cache" if not cache else ""
    # Use dev version for building prod image.
    dev_version = hlitadoc._to_dev_version(prod_version)
    cmd = rf"""
    DOCKER_BUILDKIT={DOCKER_BUILDKIT} \
    time \
    docker build \
        {opts} \
        --tag {image_versioned_prod} \
        --file {dockerfile} \
        --build-arg VERSION={dev_version} \
        --build-arg ECR_BASE_PATH={os.environ["CK_ECR_BASE_PATH"]} \
        .
    """
    hlitauti.run(ctx, cmd)
    if candidate:
        _LOG.info("Head hash: %s", head_hash)
        cmd = f"docker image ls {image_versioned_prod}"
    else:
        # Tag versioned image as latest prod image.
        latest_version = None
        image_prod = hlitadoc.get_image(base_image, "prod", latest_version)
        cmd = f"docker tag {image_versioned_prod} {image_prod}"
        hlitauti.run(ctx, cmd)
        #
        cmd = f"docker image ls {image_prod}"

    hlitauti.run(ctx, cmd)


@task
def docker_push_prod_image(  # type: ignore
    ctx,
    version,
    base_image="",
    container_dir_name=".",
):
    """
    (ONLY CI/CD) Push the "prod" image to ECR.

    :param version: version to tag the image and code with
    :param base_image: e.g., *****.dkr.ecr.us-east-1.amazonaws.com/amp
    """
    hlitauti.report_task(container_dir_name=container_dir_name)
    prod_version = hlitadoc._resolve_version_value(
        version, container_dir_name=container_dir_name
    )
    #
    hlitadoc.docker_login(ctx)
    # Push versioned tag.
    image_versioned_prod = hlitadoc.get_image(base_image, "prod", prod_version)
    cmd = f"docker push {image_versioned_prod}"
    hlitauti.run(ctx, cmd, pty=True)
    #
    latest_version = None
    image_prod = hlitadoc.get_image(base_image, "prod", latest_version)
    cmd = f"docker push {image_prod}"
    hlitauti.run(ctx, cmd, pty=True)


@task
def docker_push_prod_candidate_image(  # type: ignore
    ctx,
    candidate,
    base_image="",
    container_dir_name=".",
):
    """
    (ONLY CI/CD) Push the "prod" candidate image to ECR.

    :param candidate: hash tag of the candidate prod image to push
    :param base_image: e.g., *****.dkr.ecr.us-east-1.amazonaws.com/amp
    """
    hlitauti.report_task(container_dir_name=container_dir_name)
    #
    hlitadoc.docker_login(ctx)
    # Push image with tagged with a hash ID.
    image_versioned_prod = hlitadoc.get_image(base_image, "prod", None)
    cmd = f"docker push {image_versioned_prod}-{candidate}"
    hlitauti.run(ctx, cmd, pty=True)


@task
def docker_release_prod_image(  # type: ignore
    ctx,
    version,
    cache=True,
    skip_tests=False,
    fast_tests=True,
    slow_tests=True,
    superslow_tests=False,
    push_to_repo=True,
    container_dir_name=".",
):
    """
    (ONLY CI/CD) Build, test, and release to ECR the prod image.

    - Build prod image
    - Run the tests
    - Push the prod image repo

    :param version: version to tag the image and code with
    :param cache: use the cache
    :param skip_tests: skip all the tests and release the dev image
    :param fast_tests: run fast tests, unless all tests skipped
    :param slow_tests: run slow tests, unless all tests skipped
    :param superslow_tests: run superslow tests, unless all tests skipped
    :param push_to_repo: push the image to the repo_short_name
    """
    hlitauti.report_task(container_dir_name=container_dir_name)
    prod_version = hlitadoc._resolve_version_value(
        version, container_dir_name=container_dir_name
    )
    # 1) Build prod image.
    docker_build_prod_image(
        ctx,
        cache=cache,
        version=prod_version,
        container_dir_name=container_dir_name,
    )
    # 2) Run tests.
    if skip_tests:
        _LOG.warning("Skipping all tests and releasing")
        fast_tests = slow_tests = superslow_tests = False
    stage = "prod"
    if fast_tests:
        hlitapyt.run_fast_tests(ctx, stage=stage, version=prod_version)
    if slow_tests:
        hlitapyt.run_slow_tests(ctx, stage=stage, version=prod_version)
    if superslow_tests:
        hlitapyt.run_superslow_tests(ctx, stage=stage, version=prod_version)
    # 3) Push prod image.
    if push_to_repo:
        docker_push_prod_image(
            ctx, version=prod_version, container_dir_name=container_dir_name
        )
    else:
        _LOG.warning("Skipping pushing image to repo_short_name as requested")
    _LOG.info("==> SUCCESS <==")


@task
def docker_release_all(ctx, version, container_dir_name="."):  # type: ignore
    """
    (ONLY CI/CD) Release both dev and prod image to ECR.

    This includes:
    - docker_release_dev_image
    - docker_release_prod_image

    :param version: version to tag the image and code with
    """
    hlitauti.report_task()
    docker_release_dev_image(ctx, version, container_dir_name=container_dir_name)
    docker_release_prod_image(ctx, version, container_dir_name=container_dir_name)
    _LOG.info("==> SUCCESS <==")


def _docker_rollback_image(
    ctx: Any, base_image: str, stage: str, version: str
) -> None:
    """
    Rollback the versioned image for a particular stage.

    :param base_image: e.g., *****.dkr.ecr.us-east-1.amazonaws.com/amp
    :param stage: select a specific stage for the Docker image
    :param version: version to tag the image and code with
    """
    image_versioned_dev = hlitadoc.get_image(base_image, stage, version)
    latest_version = None
    image_dev = hlitadoc.get_image(base_image, stage, latest_version)
    cmd = f"docker tag {image_versioned_dev} {image_dev}"
    hlitauti.run(ctx, cmd)


@task
def docker_rollback_dev_image(  # type: ignore
    ctx,
    version,
    push_to_repo=True,
):
    """
    Rollback the version of the dev image.

    Phases:
    1) Ensure that version of the image exists locally
    2) Promote versioned image as dev image
    3) Push dev image to the repo

    :param version: version to tag the image and code with
    :param push_to_repo: push the image to the ECR repo
    """
    hlitauti.report_task()
    # 1) Ensure that version of the image exists locally.
    hlitadoc._docker_pull(ctx, base_image="", stage="dev", version=version)
    # 2) Promote requested image as dev image.
    _docker_rollback_image(ctx, base_image="", stage="dev", version=version)
    # 3) Push the "dev" image to ECR.
    if push_to_repo:
        docker_push_dev_image(ctx, version=version)
    else:
        _LOG.warning("Skipping pushing dev image to ECR, as requested")
    _LOG.info("==> SUCCESS <==")


@task
def docker_rollback_prod_image(  # type: ignore
    ctx,
    version,
    push_to_repo=True,
):
    """
    Rollback the version of the prod image.

    Same as parameters and meaning as `docker_rollback_dev_image`.
    """
    hlitauti.report_task()
    # 1) Ensure that version of the image exists locally.
    hlitadoc._docker_pull(ctx, base_image="", stage="prod", version=version)
    # 2) Promote requested image as prod image.
    _docker_rollback_image(ctx, base_image="", stage="prod", version=version)
    # 3) Push the "prod" image to ECR.
    if push_to_repo:
        docker_push_prod_image(ctx, version=version)
    else:
        _LOG.warning("Skipping pushing prod image to ECR, as requested")
    _LOG.info("==> SUCCESS <==")


@task
def docker_create_candidate_image(ctx, task_definition, user_tag=""):  # type: ignore
    """
    Create new prod candidate image and update the specified ECS task
    definition such that the Image URL specified in container definition points
    to the new candidate image.

    :param task_definition: the name of the ECS task definition for which an update
      to container image URL is made, e.g. cmamp-test
    :param user_tag: the name of the user creating the image, empty parameter means
      the command was run via gh actions
    """
    # Create new prod image.
    docker_build_prod_image(
        ctx,
        version=hlitadoc._IMAGE_VERSION_FROM_CHANGELOG,
        candidate=True,
        user_tag=user_tag,
    )
    # Get the hash of the image.
    tag = hgit.get_head_hash(".", short_hash=True)
    if user_tag:
        # Add user name to the candidate tag.
        tag = f"{user_tag}-{tag}"
    # Push candidate image.
    docker_push_prod_candidate_image(ctx, tag)
    exec_name = "im_v2/aws/aws_update_task_definition.py"
    # Ensure compatibility with repos where amp is a submodule.
    if not os.path.exists(exec_name):
        exec_name = f"amp/{exec_name}"
    hdbg.dassert_file_exists(exec_name)
    _LOG.debug("exec_name=%s", exec_name)
    # Register new task definition revision with updated image URL.
    cmd = f'invoke docker_cmd -c "{exec_name} -t {task_definition} -i {tag}"'
    hlitauti.run(ctx, cmd)


@task
def docker_update_prod_task_definition(ctx, version, preprod_tag, airflow_dags_s3_path, task_definition):  # type: ignore
    """
    Update image in prod task definition to the desired version.

    :param version: latest version from `changelog.txt` or custom one (e.g., `1.1.1`)
    :param preprod_tag: image that will be re-tagged with prod version
        e.g., `preprod-d8sf76s` -> `prod-1.1.1`
    :param airflow_dags_s3_path: S3 bucket from which airflow will load DAGs
    :param task_definition: which ECS task definition to use
     currently our prod ECS task definitions match short name of repos.
    """
    # TODO(Nikola): Convert `haws` part to script so it can be called via `docker_cmd`.
    #   https://github.com/cryptokaizen/cmamp/pull/2594/files#r948551787
    import helpers.haws as haws

    #
    # TODO(Nikola): Use env var for CK profile.
    s3fs_ = hs3.get_s3fs(aws_profile="ck")
    super_module = not hgit.is_inside_submodule()
    # Prepare params for listing DAGs.
    root_dir = hgit.get_client_root(super_module)
    dags_path = [root_dir, "im_v2", "airflow", "dags"]
    if super_module and hgit.is_amp_present():
        # Main DAGs location is always in `cmamp`.
        dags_path.insert(1, "amp")
    dir_name = os.path.join(*dags_path)
    pattern = "preprod.*.py"
    only_files = True
    use_relative_paths = False
    # List preprod DAGs.
    dag_paths = hs3.listdir(dir_name, pattern, only_files, use_relative_paths)
    for dag_path in dag_paths:
        # Abort in case one of the preprod DAGs is out of sync.
        _, dag_name = os.path.split(dag_path)
        hdbg.dassert_eq(
            hs3.from_file(dag_path),
            s3fs_.cat(airflow_dags_s3_path + dag_name).decode(),
            msg=f"Preprod file `{dag_name}` is out of sync with `{airflow_dags_s3_path}`!",
        )
    # Prepare params to compose new prod image url.
    prod_version = hlitadoc._resolve_version_value(version)
    base_image = ""
    stage = "prod"
    # Compose new prod image url.
    new_prod_image_url = hlitadoc.get_image(base_image, stage, prod_version)
    new_prod_image_url_no_version = hlitadoc.get_image(base_image, stage, None)
    # Check if preprod tag exist in preprod task definition as precaution.
    preprod_task_definition_name = f"{task_definition}-preprod"
    preprod_image_url = haws.get_task_definition_image_url(
        preprod_task_definition_name
    )
    preprod_tag_from_image = preprod_image_url.split(":")[-1]
    msg = f"Preprod tag is different in the image url `{preprod_tag_from_image}`!"
    hdbg.dassert_eq(preprod_tag_from_image, preprod_tag, msg=msg)
    # Pull preprod image for re-tag.
    hlitadoc.docker_login(ctx)
    cmd = f"docker pull {preprod_image_url}"
    hlitauti.run(ctx, cmd)
    # Re-tag preprod image to prod.
    cmd = f"docker tag {preprod_image_url} {new_prod_image_url}"
    hlitauti.run(ctx, cmd)
    cmd = f"docker tag {preprod_image_url} {new_prod_image_url_no_version}"
    hlitauti.run(ctx, cmd)
    cmd = f"docker rmi {preprod_image_url}"
    hlitauti.run(ctx, cmd)
    # Get original prod image for potential rollback.
    original_prod_image_url = haws.get_task_definition_image_url(task_definition)
    # Track successful uploads for potential rollback.
    successful_uploads = []
    try:
        # Update prod task definition to the latest prod tag.
        haws.update_task_definition(task_definition, new_prod_image_url)
        # Add prod DAGs to airflow s3 bucket after all checks are passed.
        for dag_path in dag_paths:
            # Update prod DAGs.
            _, dag_name = os.path.split(dag_path)
            prod_dag_name = dag_name.replace("preprod.", "prod.")
            dag_s3_path = airflow_dags_s3_path + prod_dag_name
            s3fs_.put(dag_path, dag_s3_path)
            _LOG.info("Successfully uploaded `%s`!", dag_s3_path)
            successful_uploads.append(dag_s3_path)
        # Upload new tag to ECS.
        docker_push_prod_image(ctx, prod_version)
    except Exception as ex:
        _LOG.info("Rollback started!")
        # Rollback prod task definition image URL.
        haws.update_task_definition(task_definition, original_prod_image_url)
        _LOG.info(
            "Reverted prod task definition image url to `%s`!",
            original_prod_image_url,
        )
        # Notify for potential rollback for airflow S3 bucket, if any.
        if successful_uploads:
            _LOG.warning("Starting S3 rollback!")
            # Prepare bucket resource.
            s3 = haws.get_service_resource(aws_profile="ck", service_name="s3")
            bucket_name, _ = hs3.split_path(airflow_dags_s3_path)
            bucket = s3.Bucket(bucket_name)
            for successful_upload in successful_uploads:
                # TODO(Nikola): Maybe even Telegram notification?
                # Rollback successful upload.
                _, prefix = hs3.split_path(successful_upload)
                prefix = prefix.lstrip(os.sep)
                versions = sorted(
                    bucket.object_versions.filter(Prefix=prefix),
                    key=attrgetter("last_modified"),
                    reverse=True,
                )
                latest_version = versions[0]
                latest_version.delete()
                _LOG.info("Deleted version `%s`.", latest_version.version_id)
                if len(versions) > 1:
                    rollback_version = versions[1]
                    _LOG.info(
                        "Active version is now `%s`!", rollback_version.version_id
                    )
                elif len(versions) == 1:
                    _LOG.info(
                        "Deleted version was also the only version. Nothing to rollback."
                    )
                else:
                    # TODO(Nikola): Do we need custom exception?
                    raise NotImplementedError
        s3_rollback_message = (
            f"S3 uploads reverted: {successful_uploads}"
            if successful_uploads
            else "No S3 uploads."
        )
        _LOG.info("Rollback completed! %s", s3_rollback_message)
        raise ex
