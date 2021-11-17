import logging
import os

import pytest

import helpers.git as hgit
import helpers.system_interaction as hsysinte
import helpers.unit_test as hunitest
import im.im_lib_tasks as imimlitas  # pylint: disable=no-name-in-module

_LOG = logging.getLogger(__name__)


# TODO(gp): This should come from the im_lib_tasks.py
def _get_docker_compose_file_path() -> str:
    """
    Get file path to `docker-compose.yml` file.

    :return: `docker-compose.yml` file path
    """
    amp_path = hgit.get_amp_abs_path()
    file_path = "im/devops/compose/docker-compose.yml"
    full_file_path = os.path.join(amp_path, file_path)
    return full_file_path


class TestGetImDockerCmd(hunitest.TestCase):
    def test1(self) -> None:
        """
        Test the `bash` command.
        """
        cmd = "bash"
        actual = imimlitas._get_docker_cmd(cmd)
        docker_compose_path = _get_docker_compose_file_path()
        expected = fr"""
        docker-compose \
            --file {docker_compose_path} \
            run --rm app \
            bash
        """
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test2(self) -> None:
        """
        Test the Python script.
        """
        cmd = "im/devops/docker_scripts/set_shema_im_db.py"
        actual = imimlitas._get_docker_cmd(cmd)
        docker_compose_path = _get_docker_compose_file_path()
        expected = fr"""
        docker-compose \
            --file {docker_compose_path} \
            run --rm app \
            im/devops/docker_scripts/set_shema_im_db.py
        """
        self.assert_equal(actual, expected, fuzzy_match=True)


class TestGetImDockerDown(hunitest.TestCase):
    def test1(self) -> None:
        """
        Check the command line to only remove containers.
        """
        actual = imimlitas._get_docker_down_cmd(volumes_remove=False)
        docker_compose_path = _get_docker_compose_file_path()
        expected = fr"""
        docker-compose \
            --file {docker_compose_path} \
            down
        """
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test2(self) -> None:
        """
        Check the command line to remove containers and volumes.
        """
        actual = imimlitas._get_docker_down_cmd(volumes_remove=True)
        docker_compose_path = _get_docker_compose_file_path()
        expected = fr"""
        docker-compose \
            --file {docker_compose_path} \
            down \
            -v
        """
        self.assert_equal(actual, expected, fuzzy_match=True)


# TODO(Grisha): 'is_inside_docker()' -> 'is_inside_im_container()' in #100.
@pytest.mark.skipif(hsysinte.is_inside_docker(), reason="amp #1189")
class TestImDockerCmd(hunitest.TestCase):
    def test1(self) -> None:
        """
        Test running a simple command inside `im` container.
        """
        cmd = "invoke im_docker_cmd -c ls"
        hsysinte.system(cmd)
