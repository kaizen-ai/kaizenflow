"""
Import as:

import helpers.hsql_test as hsqltest
"""

import abc
import logging
import os

import helpers.git as hgit
import helpers.printing as hprint
import helpers.sql as hsql
import helpers.system_interaction as hsysinte
import helpers.unit_test as hunitest

_LOG = logging.getLogger(__name__)


class TestDbHelper(hunitest.TestCase, abc.ABC):
    """
    This class allows to test code that interacts with DB.

    It creates / destroys a test DB during setup / teardown.

    A user can create a persistent local DB in the Docker container and then
    the creation / destruction of the DB is skipped making the tests faster
    and allowing easier debugging.
    
    The invariant is that each test should:
    - (ideally) find a clean DB to work with
    - not assume that the DB is clean. If the DB is not clean, tests should clean it
      or work around it
      - E.g., if a test needs to write a table and the table is already present and
        partially filled, as a leftover from a previous test, the new test should delete
        it and then create it again
    - clean after themselves, i.e., undo the work that has been done
      - E.g., if a test creates a table, then it should delete it at the end of the test
    """

    def setUp(self) -> None:
        """
        Initialize the test database inside test container.
        """
        _LOG.info("\n%s", hprint.frame("setUpMethod"))
        super().setUp()
        # TODO(Dan): Read the info from env in #585.
        host = "localhost"
        dbname = self._get_db_name()
        port = 5432
        user = "aljsdalsd"
        password = "alsdkqoen"
        self.dbname = dbname
        conn_exists = hsql.check_db_connection(
            host, dbname, port, user, password
        )[0]
        if conn_exists:
            _LOG.warning("DB is already up: skipping docker compose")
            # Since we have found the DB already up, we assume that we need to
            # leave it running after the tests
            self.bring_down_db = False
        else:
            # Start the service.
            self.docker_compose_file_path = os.path.join(
                hgit.get_amp_abs_path(), self._get_compose_file()
            )
            cmd = (
                "sudo docker-compose "
                f"--file {self.docker_compose_file_path} "
                f"up -d {self._get_service_name()}"
            )
            hsysinte.system(cmd, suppress_output=False)
            # Wait for the DB to be available.
            hsql.wait_db_connection(host, dbname, port, user, password)
            self.bring_down_db = True
        # Save connection info.
        self.connection = hsql.get_connection(
            host,
            self.dbname,
            port,
            user,
            password,
            autocommit=True,
        )

    def tearDown(self) -> None:
        """
        Bring down the test container.
        """
        _LOG.info("\n%s", hprint.frame("tearDown"))
        if self.bring_down_db:
            cmd = (
                "sudo docker-compose "
                f"--file {self.docker_compose_file_path} down -v"
            )
            hsysinte.system(cmd, suppress_output=False)
        else:
            _LOG.warning("Leaving DB up")
        super().tearDown()

    @staticmethod
    @abc.abstractmethod
    def _get_compose_file() -> str:
        """
        Get path to Docker compose file.
        """

    # TODO(Dan): Deprecate after #585.
    @staticmethod
    @abc.abstractmethod
    def _get_db_name() -> str:
        """
        Get DB name.
        """

    @staticmethod
    @abc.abstractmethod
    def _get_service_name() -> str:
        """
        Get service name.
        """


class TestOmsDbHelper(TestDbHelper):
    """
    This class allows to test code that interacts with OMS DB.

    A user can create a persistent local DB in the Docker container with:
    ```
    # Create an OMS DB inside Docker for local stage
    docker> (cd oms; sudo docker-compose \
        --file /app/oms/devops/compose/docker-compose.yml up \
        -d \
        oms_postgres_local)
    # or
    docker> invoke oms_docker_up
    ```
    """

    @staticmethod
    def _get_compose_file() -> str:
        # TODO(gp): This information should be retrieved from oms_lib_tasks.py.
        #  We can also use the invoke command.
        return "oms/devops/compose/docker-compose.yml"

    # TODO(Dan): Deprecate after #585.
    @staticmethod
    def _get_db_name() -> str:
        return "oms_postgres_db_local"

    @staticmethod
    def _get_service_name() -> str:
        return "oms_postgres_local"
