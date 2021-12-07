"""
Import as:

import helpers.hsql_test as hsqltest
"""

import logging
import os

import helpers.git as hgit
import helpers.printing as hprint
import helpers.sql as hsql
import helpers.system_interaction as hsysinte
import helpers.unit_test as hunitest

_LOG = logging.getLogger(__name__)


class TestOmsDbHelper(hunitest.TestCase):
    """
    This class allows to test code that interacts with DB.

    It creates / destroys a test DB during setup / teardown.

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
    and then the creation / destruction of the DB is skipped making the tests faster
    and allowing easier debugging.
    For this to work, tests should not assume that the DB is clean, but create tables
    from scratch.
    """

    def setUp(self) -> None:
        """
        Initialize the test database inside test container.
        """
        _LOG.info("\n%s", hprint.frame("setUp"))
        super().setUp()
        # TODO(gp): Read the info from env.
        host = "localhost"
        dbname = "oms_postgres_db_local"
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
            # TODO(gp): This information should be retrieved from oms_lib_tasks.py.
            #  We can also use the invoke command.
            self.docker_compose_file_path = os.path.join(
                hgit.get_amp_abs_path(), "oms/devops/compose/docker-compose.yml"
            )
            cmd = (
                "sudo docker-compose "
                f"--file {self.docker_compose_file_path} "
                "up -d oms_postgres_local"
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
