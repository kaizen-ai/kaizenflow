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
    not hgit.execute_repo_config_code("has_dind_support()"),
    reason="Need dind support",
)
class TestDbHelper(hunitest.TestCase, abc.ABC):
    """
    This class allows testing code that interacts with a DB.

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
        # Read the connection parameters from the env file.
        cls.db_env_file = cls._get_db_env_path()
        connection_info = hsql.get_connection_info_from_env_file(cls.db_env_file)
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
        if cls.bring_down_db:
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

    @staticmethod
    @abc.abstractmethod
    def _get_compose_file() -> str:
        """
        Get path to Docker compose file.
        """

    @staticmethod
    @abc.abstractmethod
    def _get_service_name() -> str:
        """
        Get service name.
        """

    @staticmethod
    @abc.abstractmethod
    def _get_db_env_path() -> str:
        """
        Get path to db env file that contains db connection parameters.
        """
