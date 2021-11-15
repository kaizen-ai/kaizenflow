import logging
import os

import helpers.printing as hprint
import helpers.sql as hsql
import helpers.system_interaction as hsysinte
import helpers.unit_test as hunitest

_LOG = logging.getLogger(__name__)


# TODO(gp): Generalize and move this to hsql.py or hsql_test.py.
class _TestOmsDbHelper(hunitest.TestCase):
    def setUp(self) -> None:
        """
        Initialize the test database inside test container.
        """
        super().setUp()
        _LOG.info("\n%s", hprint.frame("setUp"))
        # Start the service.
        cmd = []
        # TODO(gp): This information should be retrieved from oms_lib_tasks.py.
        #  We can also use the invoke command.
        self.docker_compose_file_path = os.path.abspath(
            "oms/devops/compose/docker-compose.yml"
        )
        cmd.append("sudo docker-compose")
        cmd.append(f"--file {self.docker_compose_file_path}")
        service = "oms_postgres_local"
        cmd.append(f"up -d {service}")
        cmd = " ".join(cmd)
        hsysinte.system(cmd, suppress_output=False)
        # TODO(gp): Read the info from env.
        dbname = "oms_postgres_db_local"
        host = "localhost"
        port = 5432
        password = "alsdkqoen"
        user = "aljsdalsd"
        # Wait for the DB to be available.
        hsql.wait_db_connection(dbname, port, host)
        # Save connection info.
        self.connection, self.cursor = hsql.get_connection(
            dbname,
            host,
            user,
            port,
            password,
            autocommit=True,
        )

    def tearDown(self) -> None:
        """
        Bring down the test container.
        """
        _LOG.info("\n%s", hprint.frame("tearDown"))
        cmd = (
            "sudo docker-compose "
            f"--file {self.docker_compose_file_path} down -v"
        )
        hsysinte.system(cmd, suppress_output=False)
        super().tearDown()


# #############################################################################


class TestOmsDb1(_TestOmsDbHelper):
    def test_up1(self) -> None:
        """
        Verify that the DB is up.
        """
        db_list = hsql.get_db_names(self.connection)
        _LOG.info("db_list=%s", db_list)
