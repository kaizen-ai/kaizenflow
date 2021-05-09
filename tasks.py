import logging

# Expose the pytest targets.
# TODO(gp): Write a script to extract the targets programmatically, for now
#  they are extracted with:
# > \grep "^@task" -A 1 helpers/lib_tasks.py | grep def
from helpers.lib_tasks import (
    docker_bash,
    docker_build_local_image,
    docker_build_prod_image,
    docker_cmd,
    docker_images_ls_repo,
    docker_jupyter,
    docker_kill_all,
    docker_kill_last,
    docker_login,
    docker_ps,
    docker_pull,
    docker_push_local_image_to_dev,
    docker_release_all,
    docker_release_dev_image,
    docker_release_prod_image,
    docker_stats,
    find_test_class,
    find_test_decorator,
    get_amp_files,
    gh_create_pr,
    gh_issue_title,
    gh_workflow_list,
    gh_workflow_run,
    git_branch_files,
    git_clean,
    git_create_branch,
    git_delete_merged_branches,
    git_merge_master,
    git_pull,
    git_pull_master,
    jump_to_pytest_error,
    lint,
    print_setup,
    pytest_clean,
    run_blank_tests,
    run_fast_slow_tests,
    run_fast_tests,
    run_slow_tests,
    run_superslow_tests,
    #
    set_default_params
    )
import helpers.versioning as hversi


_LOG = logging.getLogger(__name__)


# #############################################################################
# Setup.
# #############################################################################


hversi.check_version("./version.txt")

# TODO(gp): Move it to lib_tasks.
ECR_BASE_PATH = "665840871993.dkr.ecr.us-east-1.amazonaws.com"


default_params = {
    "ECR_BASE_PATH": ECR_BASE_PATH,
    # When testing a change to the build system in a branch you can use a different
    # image, e.g., `XYZ_tmp` to not interfere with the prod system.
    # "BASE_IMAGE": "amp_tmp",
    "BASE_IMAGE": "amp",
    "DEV_TOOLS_IMAGE_PROD": f"{ECR_BASE_PATH}/dev_tools:prod"
}


set_default_params(default_params)
