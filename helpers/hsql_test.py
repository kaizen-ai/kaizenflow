"""
Import as:

import helpers.hsql_test as hsqltest
"""

import abc
import logging
import os

import pytest

import helpers.hgit as hgit
import helpers.hprint as hprint
import helpers.hsql as hsql
import helpers.hsystem as hsystem
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)


@pytest.mark.skipif(
    not hgit.execute_repo_config_code("has_dind_support()") and
    not hgit.execute_repo_config_code("use_docker_sibling_containers()"),
    reason="Need docker children / sibling support",
)
class TestDbHelper(hunitest.TestCase, abc.ABC):
    """
    Allow testing code that interacts with a DB.

    It creates / destroys a test DB during setup / teardown of the class. This means
    that the same DB is reused for multiple test methods of the same class.

    The invariant is that each test method should:
    - (ideally) find a clean DB to work with
    - not assume that the DB is clean. If the DB is not clean, tests should clean it
      before starting, or work around it
      - E.g., if a test needs to write a table, but the table is already present and
        partially filled as a leftover from a previous test, the new test should
        delete the table and create it again
    - clean the DB after themselves, i.e., undo the work that has been done
      - E.g., if a test creates a table, then the test should delete the table at
        the end of the test

    - An existing DB can be reused
      - A user can create a persistent local DB in the Docker container, e.g. for OMS:
        ```
        docker> (cd oms; sudo docker-compose \
                    --file /app/oms/devops/compose/docker-compose.yml up \
                    -d \
                    oms_postgres)
        ```
        or
        ```
        docker> invoke oms_docker_up
        ```
      - Then this class skips creating / destructing the DB, making the tests faster
        and allowing easier debugging.
    """

    @classmethod
    def setUpClass(cls) -> None:
        """
        Initialize the test database inside test container.
        """
        _LOG.info("\n%s", hprint.frame("setUpClass"))
        cls._create_docker_files()
        # Read the connection parameters from the env file.
        cls.db_env_file = cls._get_db_env_path()
        connection_info = hsql.get_connection_info_from_env_file(cls.db_env_file)
        _LOG.debug("connection_info=%s", connection_info)
        conn_exists = hsql.check_db_connection(*connection_info)[0]
        if conn_exists:
            _LOG.warning("DB is already up: skipping docker compose")
            # Since we have found the DB already up, we assume that we need to
            # leave it running after the tests
            cls.bring_down_db = False
        else:
            # Start the service.
            cls.docker_compose_file_path = os.path.join(
                hgit.get_amp_abs_path(), cls._get_compose_file()
            )
            # TODO(Grisha): use invoke task CMTask #547.
            cmd = (
                "sudo docker-compose "
                f"--file {cls.docker_compose_file_path} "
                f"--env-file {cls.db_env_file} "
                f"up -d {cls._get_service_name()}"
            )
            _LOG.debug("cmd=%s", cmd)
            hsystem.system(cmd, suppress_output=False)
            # Wait for the DB to be available.
            hsql.wait_db_connection(*connection_info)
            cls.bring_down_db = True
        # Save connection info.
        # TODO(gp): -> db_connection
        cls.connection = hsql.get_connection(*connection_info, autocommit=True)

    @classmethod
    def tearDownClass(cls) -> None:
        """
        Bring down the test container.
        """
        _LOG.info("\n%s", hprint.frame("tearDown"))
        docker_compose_cleanup = cls.bring_down_db
        if hgit.execute_repo_config_code("use_main_network()"):
            # TODO(gp): When using sibling containers `docker-compose down` tries to
            #  shut down also the `main_network`, while it is attached to the Docker
            #  container running the tests.
            #  > docker network inspect main_network
            #  We should clean up the containers and volumes directly. We can put
            #  this in a invoke target.
            docker_compose_cleanup = False
        if docker_compose_cleanup:
            # TODO(Grisha): use invoke task CMTask #547.
            cmd = (
                "sudo docker-compose "
                f"--file {cls.docker_compose_file_path} "
                f"--env-file {cls.db_env_file} "
                "down -v"
            )
            hsystem.system(cmd, suppress_output=False)
        else:
            _LOG.warning("Leaving DB up")
        if not hunitest.get_incremental_tests():
            os.unlink(cls._get_compose_file())
            os.unlink(cls._get_db_env_path())

    @classmethod
    @abc.abstractmethod
    def get_id(cls) -> int:
        """
        Return a unique ID to create an OMS instance.

        This ID is used to generate Docker compose / env files and services, so
        that we can avoid collisions in case of parallel execution.

        This function is specified by the unit test in a way that is
        unique to each test.
        """
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def _get_compose_file(cls) -> str:
        """
        Get path to Docker compose file.
        """
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def _get_service_name(cls) -> str:
        """
        Get service name.
        """
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def _get_db_env_path(cls) -> str:
        """
        Get path to env file that contains DB connection parameters.
        """
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def _create_docker_files(cls) -> str:
        """
        Create the compose and env file for the DB run.
        """
        raise NotImplementedError


# TODO(gp): @all Create a class sharing some common code from TestOmsDbHelper
#  and TestImDbHelper.
# class TestImOmsDbHelper(hunitest.TestCase, abc.ABC):
