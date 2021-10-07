import pytest

import helpers.system_interaction as hsyint
import helpers.unit_test as huntes
import im.im_lib_tasks as imimlitas  # pylint: disable=no-name-in-module


class TestGetImDockerCmd(huntes.TestCase):
    def test1(self) -> None:
        """
        Check the command line to run `bash` on the `local` stage.
        """
        cmd = "bash"
        stage = "local"
        actual = imimlitas._get_im_docker_cmd(stage, cmd)
        expected = r"""
        docker-compose \
            --file /app/im/devops/compose/docker-compose.yml \
            --file /app/im/devops/compose/docker-compose.local.yml \
            run --rm app \
            bash
        """
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test2(self) -> None:
        """
        Check the command line to run Python script on the `dev` stage.
        """
        cmd = "im/devops/docker_scripts/set_shema_im_db.py"
        stage = "dev"
        actual = imimlitas._get_im_docker_cmd(stage, cmd)
        expected = r"""
        docker-compose \
            --file /app/im/devops/compose/docker-compose.yml \
            --file /app/im/devops/compose/docker-compose.dev.yml \
            run --rm app \
            im/devops/docker_scripts/set_shema_im_db.py
        """
        self.assert_equal(actual, expected, fuzzy_match=True)


class TestGetImDockerDown(huntes.TestCase):
    def test1(self) -> None:
        """
        Check the command line to only remove containers on the `prod` stage.
        """
        actual = imimlitas._get_im_docker_down(stage="prod", volumes_remove=False)
        expected = r"""
        docker-compose \
            --file /app/im/devops/compose/docker-compose.yml \
            --file /app/im/devops/compose/docker-compose.prod.yml \
            down
        """
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test2(self) -> None:
        """
        Check the command line to remove containers and volumes on the `dev`
        stage.
        """
        actual = imimlitas._get_im_docker_down(stage="dev", volumes_remove=True)
        expected = r"""
        docker-compose \
            --file /app/im/devops/compose/docker-compose.yml \
            --file /app/im/devops/compose/docker-compose.dev.yml \
            down \
            -v
        """
        self.assert_equal(actual, expected, fuzzy_match=True)


# TODO(Grisha): 'is_inside_docker()' -> 'is_inside_im_container()' in #100.
@pytest.mark.skipif(hsyint.is_inside_docker(), reason="amp #1189")
class TestImDockerCmd(huntes.TestCase):
    def test1(self) -> None:
        """
        Test running a simple command inside `im` container.
        """
        cmd = "invoke im_docker_cmd -c ls"
        hsyint.system(cmd)
