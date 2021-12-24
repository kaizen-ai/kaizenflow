import logging
import os
from typing import Dict

import pytest

import helpers.system_interaction as hsysinte
import helpers.unit_test as hunitest

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


@pytest.mark.qa
@pytest.mark.skipif(hsysinte.is_inside_docker(), reason="AmpTask165")
class TestExecuteTasks1(hunitest.TestCase):
    """
    Execute tasks that don't change state of the system (e.g., commit images).
    """

    @pytest.fixture(autouse=True)
    def inject_config(self, request):
        self._config = request.config

    def test_list(self) -> None:
        cmd = "invoke --list"
        hsysinte.system(cmd)

    def test_print_setup1(self) -> None:
        cmd = "invoke print_setup"
        hsysinte.system(cmd)

    def test_docker_images_ls_repo1(self) -> None:
        cmd = "invoke docker_images_ls_repo"
        hsysinte.system(cmd)

    def test_docker_ps(self) -> None:
        cmd = "invoke docker_ps"
        hsysinte.system(cmd)

    def test_docker_stats(self) -> None:
        cmd = "invoke docker_stats"
        hsysinte.system(cmd)

    def test_docker_login1(self) -> None:
        cmd = "invoke docker_login"
        hsysinte.system(cmd)

    def test_docker_cmd1(self) -> None:
        cmd = 'invoke docker_cmd --cmd="ls"'
        hsysinte.system(cmd)

    def test_docker_jupyter1(self) -> None:
        cmd = "invoke docker_jupyter --self-test"
        hsysinte.system(cmd)

    def test_docker_bash(self) -> None:
        image_version = self._config.getoption("--image_version")
        image_stage = self._config.getoption("--image_stage")
        exit_command = "<(echo 'exit\n')"
        cmd = f"invoke docker_bash"
        if image_version:
            cmd = f"{cmd} --version {image_version}"
        cmd = f"{cmd} --stage {image_stage} < {exit_command}"
        hsysinte.system(cmd)


@pytest.mark.qa
@pytest.mark.skipif(hsysinte.is_inside_docker(), reason="AmpTask165")
class TestExecuteTasks2(hunitest.TestCase):
    """
    Execute tasks that change the state of the system but use a temporary
    image.
    """

    def test_docker_jupyter1(self) -> None:
        cmd = "invoke docker_jupyter --self-test"
        hsysinte.system(cmd)

    def test_docker_pull1(self) -> None:
        cmd = "invoke docker_pull"
        hsysinte.system(cmd)

    # Images workflows.

    def test_docker_build_local_image(self) -> None:
        params = _get_default_params()
        base_image = params["ECR_BASE_PATH"] + "/" + params["BASE_IMAGE"]
        cmd = f"invoke docker_build_local_image --version 1.0.0 --cache --base-image={base_image}"
        hsysinte.system(cmd)

    @pytest.mark.skip("No prod image for amp yet")
    def test_docker_build_prod_image(self) -> None:
        params = _get_default_params()
        base_image = params["ECR_BASE_PATH"] + "/" + params["BASE_IMAGE"]
        cmd = f"invoke docker_build_prod_image --version 1.0.0 --cache --base-image={base_image}"
        hsysinte.system(cmd)

    # Run tests.

    def test_run_blank_tests1(self) -> None:
        cmd = "invoke run_blank_tests"
        hsysinte.system(cmd)

    @pytest.mark.skip
    @pytest.mark.slow("Around 30 secs")
    def test_collect_only1(self) -> None:
        cmd = "invoke docker_cmd --cmd='pytest --collect-only'"
        hsysinte.system(cmd)

    def test_collect_only2(self) -> None:
        # We need to specify the dir independently of the git root since this will
        # run inside a container.
        dir_name = '$(dirname $(find . -name "test_dbg.py" -type f))'
        cmd = f"invoke docker_cmd --cmd='pytest {dir_name} --collect-only'"
        hsysinte.system(cmd)

    def test_run_fast_tests(self) -> None:
        file_name = '$(find . -name "test_dbg.py" -type f)'
        cmd = f"invoke run_fast_tests --pytest-opts='{file_name}'"
        hsysinte.system(cmd)

    def test_run_fast_tests_failed(self) -> None:
        file_name = "helpers/test/test_lib_tasks.py::TestFailing"
        cmd = f"export AM_FORCE_TEST_FAIL=1; invoke run_fast_tests --pytest-opts='{file_name}'"
        with self.assertRaises(RuntimeError):
            hsysinte.system(cmd)

    # Linter.

    def test_lint1(self) -> None:
        # Get the pointer to amp.
        file_name = '$(find . -name "dbg.py" -type f)'
        cmd = f"invoke lint --files='{file_name}' --phases='black'"
        hsysinte.system(cmd)
