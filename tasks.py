import logging
import os

import helpers.versioning as hversi

# Expose the pytest targets.
# Extract with:
# > i print_tasks --as-code
from helpers.lib_tasks import set_default_params  # This is not an invoke target.
from helpers.lib_tasks import (  # noqa: F401  # pylint: disable=unused-import
    check_python_files,
    docker_bash,
    docker_build_local_image,
    docker_build_prod_image,
    docker_cmd,
    docker_images_ls_repo,
    docker_jupyter,
    docker_kill,
    docker_login,
    docker_ps,
    docker_pull,
    docker_push_dev_image,
    docker_release_all,
    docker_release_dev_image,
    docker_release_prod_image,
    docker_stats,
    docker_tag_local_image_as_dev,
    find_check_string_output,
    find_test_class,
    find_test_decorator,
    fix_perms,
    gh_create_pr,
    gh_issue_title,
    gh_workflow_list,
    gh_workflow_run,
    git_branch_files,
    git_clean,
    git_create_branch,
    git_create_patch,
    git_delete_merged_branches,
    git_files,
    git_last_commit_files,
    git_merge_master,
    git_pull,
    git_pull_master,
    git_rename_branch,
    lint,
    print_setup,
    print_tasks,
    pytest_clean,
    pytest_failed,
    pytest_failed_freeze_test_list,
    run_blank_tests,
    run_fast_slow_tests,
    run_fast_tests,
    run_slow_tests,
    run_superslow_tests,
    traceback,
)
from im_v2.im_lib_tasks import (  # noqa: F401  # pylint: disable=unused-import
    im_docker_cmd,
    im_docker_down,
    im_docker_up,
)
import repo_config as rconf

_LOG = logging.getLogger(__name__)


# #############################################################################
# Setup.
# #############################################################################


hversi.check_version("./version.txt")

# TODO(gp): Move it to lib_tasks.
ECR_BASE_PATH = os.environ["AM_ECR_BASE_PATH"]
DOCKER_BASE_IMAGE_NAME = rconf.get_docker_base_image_name()


def docker_release_end_to_end_test(*args, **kwargs):
    """
    Dummy no-op function that mimics end-to-end test that always passes.
    Used in docker_release_dev_image.
    """
    return True


default_params = {
    "ECR_BASE_PATH": ECR_BASE_PATH,
    # When testing a change to the build system in a branch you can use a different
    # image, e.g., `XYZ_tmp` to not interfere with the prod system.
    # "BASE_IMAGE": "amp_tmp",
    "BASE_IMAGE": DOCKER_BASE_IMAGE_NAME,
    "DEV_TOOLS_IMAGE_PROD": f"{ECR_BASE_PATH}/dev_tools:prod",
    "END_TO_END_TEST_FN": docker_release_end_to_end_test,
}


set_default_params(default_params)
