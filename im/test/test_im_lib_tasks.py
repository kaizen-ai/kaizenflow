import helpers.system_interaction as hsinte
import helpers.unit_test as huntes
import im.im_lib_tasks as imimlitas  # pylint: disable=no-name-in-module


class TestGetImDockerCmd(huntes.TestCase):
    def test1(self) -> None:
        """
        Test the `bash` command.
        """
        cmd = "bash"
        actual = imimlitas._get_im_docker_cmd(cmd)
        expected = r"""
        docker-compose \
            --file /app/im/devops/compose/docker-compose.yml \
            run --rm app \
            bash
        """
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test2(self) -> None:
        """
        Test the Python script.
        """
        cmd = "im/devops/docker_scripts/set_shema_im_db.py"
        actual = imimlitas._get_im_docker_cmd(cmd)
        expected = r"""
        docker-compose \
            --file /app/im/devops/compose/docker-compose.yml \
            run --rm app \
            im/devops/docker_scripts/set_shema_im_db.py
        """
        self.assert_equal(actual, expected, fuzzy_match=True)


class TestImDockerCmd(huntes.TestCase):
    def test1(self) -> None:
        """
        Test running a simple command inside `im` container.
        """
        cmd = "invoke im_docker_cmd -c ls"
        hsinte.system(cmd)
