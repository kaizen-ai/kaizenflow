import logging

import pytest

import helpers.lib_tasks as hlibtask
import helpers.system_interaction as hsysinte
import helpers.unit_test as hunitest
import im_v2.im_lib_tasks as imimlitas  # pylint: disable=no-name-in-module

_LOG = logging.getLogger(__name__)


class TestGetImDockerCmd(hunitest.TestCase):
    def test1(self) -> None:
        """
        Test the `bash` command.
        """
        cmd = "bash"
        actual = imimlitas._get_docker_cmd(cmd)
        docker_compose_path = hlibtask.get_base_docker_compose_path()
        expected = fr"""
        docker-compose \
            --file {docker_compose_path} \
            run --rm im_app \
            bash
        """
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test2(self) -> None:
        """
        Test the Python script.
        """
        cmd = "im/devops/docker_scripts/set_shema_im_db.py"
        actual = imimlitas._get_docker_cmd(cmd)
        docker_compose_path = hlibtask.get_base_docker_compose_path()
        expected = fr"""
        docker-compose \
            --file {docker_compose_path} \
            run --rm im_app \
            im/devops/docker_scripts/set_shema_im_db.py
        """
        self.assert_equal(actual, expected, fuzzy_match=True)


class TestGetImDockerBashCmd(hunitest.TestCase):
    def test1(self) -> None:
        """
        Test the bash.
        """
        actual = imimlitas._get_docker_bash()
        docker_compose_path = hlibtask.get_base_docker_compose_path()
        expected = fr"""
        docker-compose \
            --file {docker_compose_path} \
            run --rm im_app \
            bash
        """
        self.assert_equal(actual, expected, fuzzy_match=True)


class TestGetImDockerDown(hunitest.TestCase):
    def test1(self) -> None:
        """
        Check the command line to only remove containers.
        """
        actual = imimlitas._get_docker_down_cmd(volumes_remove=False)
        docker_compose_path = hlibtask.get_base_docker_compose_path()
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
        docker_compose_path = hlibtask.get_base_docker_compose_path()
        expected = fr"""
        docker-compose \
            --file {docker_compose_path} \
            down \
            -v
        """
        self.assert_equal(actual, expected, fuzzy_match=True)


class TestGetImDockerUp(hunitest.TestCase):
    def test1(self) -> None:
        """
        Check the command line to bring up the db.
        """
        actual = imimlitas._get_docker_up_cmd(detach=False)
        docker_compose_path = hlibtask.get_base_docker_compose_path()
        expected = fr"""
        docker-compose \
            --file {docker_compose_path} \
            up \
            im_postgres_local
        """
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test2(self) -> None:
        """
        Check the command line to bring up the db in the detached mode.
        """
        actual = imimlitas._get_docker_up_cmd(detach=True)
        docker_compose_path = hlibtask.get_base_docker_compose_path()
        expected = fr"""
        docker-compose \
            --file {docker_compose_path} \
            up \
            -d \
            im_postgres_local
        """
        self.assert_equal(actual, expected, fuzzy_match=True)


class TestGetCreateDbCmd(hunitest.TestCase):
    def test1(self) -> None:
        """
        Test the `create_db` script.
        """
        actual = imimlitas._get_create_db_cmd(
            dbname="test_db",
            overwrite=False,
            credentials="from_env"
        )
        docker_compose_path = hlibtask.get_base_docker_compose_path()
        expected = fr"""
        docker-compose \
            --file {docker_compose_path} \
            run --rm im_app \
            im_v2/common/db/create_db.py \
            --db-name 'test_db' \
            --credentials '"from_env"'
        """
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test2(self) -> None:
        """
        Test the `create_db` script with overwrite option passed.
        """
        actual = imimlitas._get_create_db_cmd(
            dbname="test_db",
            overwrite=True,
            credentials="from_env"
        )
        docker_compose_path = hlibtask.get_base_docker_compose_path()
        expected = fr"""
        docker-compose \
            --file {docker_compose_path} \
            run --rm im_app \
            im_v2/common/db/create_db.py \
            --db-name 'test_db' \
            --overwrite \
            --credentials '"from_env"'
        """
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test3(self) -> None:
        """
        Test the `create_db` script with credentials via json file.
        """
        actual = imimlitas._get_create_db_cmd(
            dbname="test_db",
            overwrite=False,
            credentials="test.json"
        )
        docker_compose_path = hlibtask.get_base_docker_compose_path()
        expected = fr"""
        docker-compose \
            --file {docker_compose_path} \
            run --rm im_app \
            im_v2/common/db/create_db.py \
            --db-name 'test_db' \
            --credentials 'test.json'
        """

    def test4(self) -> None:
        """
        Test the `create_db` script with credentials from string.
        """
        actual = imimlitas._get_create_db_cmd(
            dbname="test_db", 
            overwrite=False, 
            credentials="host=localhost dbname=im_postgres_db_local port=54",
        )
        docker_compose_path = hlibtask.get_base_docker_compose_path()
        expected = fr"""
        docker-compose \
            --file {docker_compose_path} \
            run --rm im_app \
            im_v2/common/db/create_db.py \
            --db-name 'test_db' \
            --credentials '"host=localhost dbname=im_postgres_db_local port=54"'
        """


class TestGetRemoveDbCmd(hunitest.TestCase):
    def test1(self) -> None:
        """
        Test the `remove_db` script.
        """
        actual = imimlitas._get_remove_db_cmd(
            dbname="test_db",
            credentials="from_env"
        )
        docker_compose_path = hlibtask.get_base_docker_compose_path()
        expected = fr"""
        docker-compose \
            --file {docker_compose_path} \
            run --rm im_app \
            im_v2/common/db/remove_db.py \
            --db-name 'test_db' \
            --credentials '"from_env"'
        """
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test2(self) -> None:
        """
        Test the `remove_db` script with connection from string.
        """
        actual = imimlitas._get_remove_db_cmd(
            dbname="test_db",
            credentials="host=localhost dbname=im_postgres_db_local port=54",
        )
        docker_compose_path = hlibtask.get_base_docker_compose_path()
        expected = fr"""
        docker-compose \
            --file {docker_compose_path} \
            run --rm im_app \
            im_v2/common/db/remove_db.py \
            --db-name 'test_db' \
            --credentials '"host=localhost dbname=im_postgres_db_local port=54"'
        """
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test3(self) -> None:
        """
        Test the `remove_db` script with path to json file.
        """
        actual = imimlitas._get_remove_db_cmd(
            dbname="test_db",
            credentials="asd.json",
        )
        docker_compose_path = hlibtask.get_base_docker_compose_path()
        expected = fr"""
        docker-compose \
            --file {docker_compose_path} \
            run --rm im_app \
            im_v2/common/db/remove_db.py \
            --db-name 'test_db' \
            --credentials asd.json
        """


# TODO(Grisha): 'is_inside_docker()' -> 'is_inside_im_container()' in #100.
@pytest.mark.skipif(hsysinte.is_inside_docker(), reason="amp #1189")
class TestImDockerCmd(hunitest.TestCase):
    def test1(self) -> None:
        """
        Test running a simple command inside `im` container.
        """
        cmd = "invoke im_docker_cmd -c ls"
        hsysinte.system(cmd)
