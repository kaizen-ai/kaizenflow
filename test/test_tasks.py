import logging
import os
from typing import Dict

import pytest

import helpers.system_interaction as hsinte
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


def _get_default_params() -> Dict[str, str]:
    """
    Get fake params pointing to a different image so we can test the code
    without affecting the official images.
    """
    ecr_base_path = os.environ["AM_ECR_BASE_PATH"]
    default_params = {
        "ECR_BASE_PATH": ecr_base_path,
        "BASE_IMAGE": "amp_test",
        "DEV_TOOLS_IMAGE_PROD": f"{ecr_base_path}/dev_tools:prod",
    }
    return default_params


@pytest.mark.no_container
@pytest.mark.skipif(hsinte.is_inside_docker(), reason="AmpTask165")
class TestExecuteTasks1(hut.TestCase):
    """
    Execute tasks that don't change state of the system (e.g., commit images).
    """

    def test_list(self) -> None:
        cmd = "invoke --list"
        hsinte.system(cmd)

    def test_print_setup1(self) -> None:
        cmd = "invoke print_setup"
        hsinte.system(cmd)

    def test_docker_images_ls_repo1(self) -> None:
        cmd = "invoke docker_images_ls_repo"
        hsinte.system(cmd)

    def test_docker_ps(self) -> None:
        cmd = "invoke docker_ps"
        hsinte.system(cmd)

    def test_docker_stats(self) -> None:
        cmd = "invoke docker_stats"
        hsinte.system(cmd)

    def test_docker_login1(self) -> None:
        cmd = "invoke docker_login"
        hsinte.system(cmd)

    def test_docker_cmd1(self) -> None:
        cmd = 'invoke docker_cmd --cmd="ls"'
        hsinte.system(cmd)

    def test_docker_jupyter1(self) -> None:
        cmd = "invoke docker_jupyter --self-test"
        hsinte.system(cmd)

    def test_im_docker_cmd(self) -> None:
        """
        Test running a simple command inside `im` container.
        """
        cmd = "invoke im_docker_cmd -c ls"
        hsinte.system(cmd)


@pytest.mark.no_container
@pytest.mark.skipif(hsinte.is_inside_docker(), reason="AmpTask165")
class TestExecuteTasks2(hut.TestCase):
    """
    Execute tasks that change the state of the system but use a temporary
    image.
    """

    def test_docker_jupyter1(self) -> None:
        cmd = "invoke docker_jupyter --self-test"
        hsinte.system(cmd)

    def test_docker_pull1(self) -> None:
        cmd = "invoke docker_pull"
        hsinte.system(cmd)

    # Images workflows.

    def test_docker_build_local_image(self) -> None:
        params = _get_default_params()
        base_image = params["ECR_BASE_PATH"] + "/" + params["BASE_IMAGE"]
        cmd = f"invoke docker_build_local_image --cache --base-image={base_image}"
        hsinte.system(cmd)

    @pytest.mark.skip("No prod image for amp yet")
    def test_docker_build_prod_image(self) -> None:
        params = _get_default_params()
        base_image = params["ECR_BASE_PATH"] + "/" + params["BASE_IMAGE"]
        cmd = f"invoke docker_build_prod_image --cache --base-image={base_image}"
        hsinte.system(cmd)

    # Run tests.

    def test_run_blank_tests1(self) -> None:
        cmd = "invoke run_blank_tests"
        hsinte.system(cmd)

    @pytest.mark.skip
    @pytest.mark.slow("Around 30 secs")
    def test_collect_only1(self) -> None:
        cmd = "invoke docker_cmd --cmd='pytest --collect-only'"
        hsinte.system(cmd)

    def test_collect_only2(self) -> None:
        # We need to specify the dir independently of the git root since this will
        # run inside a container.
        dir_name = '$(dirname $(find . -name "test_dbg.py" -type f))'
        cmd = f"invoke docker_cmd --cmd='pytest {dir_name} --collect-only'"
        hsinte.system(cmd)

    def test_run_fast_tests(self) -> None:
        file_name = '$(find . -name "test_dbg.py" -type f)'
        cmd = f"invoke run_fast_tests --pytest-opts='{file_name}'"
        hsinte.system(cmd)

    # Linter.

    def test_lint_docker_pull1(self) -> None:
        cmd = "invoke lint_docker_pull"
        hsinte.system(cmd)

    def test_lint1(self) -> None:
        # Get the pointer to amp.
        file_name = '$(find . -name "dbg.py" -type f)'
        cmd = f"invoke lint --files='{file_name}' --phases='black'"
        hsinte.system(cmd)
