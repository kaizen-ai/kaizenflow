import os
import logging

import pytest

import helpers.git as hgit
import helpers.sql as hsql
import helpers.system_interaction as hsyint
import helpers.unit_test as huntes
import im.common.db.create_db as imcodbcrdb

_LOG = logging.getLogger(__name__)


@pytest.mark.skipif(not hgit.is_amp(), reason="Only run in amp")
class Test_sql(huntes.TestCase):
    def setUp(self):
        """
        Initialize the test container.
        """
        super().setUp()
        self.docker_compose_file_path = os.path.join(
            hgit.get_amp_abs_path(),
            "im/devops/compose/docker-compose.yml"
        )
        cmd = (
            "sudo docker-compose "
            f"--file {self.docker_compose_file_path} "
            "up -d im_postgres_local"
        )
        hsyint.system(cmd, suppress_output=False)
        
    def tearDown(self):
        """
        Bring down the test container.
        """
        cmd = ("sudo docker-compose "
               f"--file {self.docker_compose_file_path} down -v")
        hsyint.system(cmd, suppress_output=False)

        super().tearDown()

    @pytest.mark.slow()
    def test_waitdb(self) -> None:
        """
        Smoke test.
        """
        #TODO(Dan3): change to env
        dbname = "im_postgres_db_local"
        host = "localhost"
        port = 5432
        hsql.wait_db_connection(dbname, port, host)

    @pytest.mark.slow()
    def test_db_connection_to_tuple(self) -> None:
        """
        Verify that connection string is correct.
        """
        dbname = "im_postgres_db_local"
        host = "localhost"
        port = 5432
        password = "alsdkqoen"
        user = "aljsdalsd"
        hsql.wait_db_connection(dbname, port, host)
        self.connection, _ = hsql.get_connection(
            dbname,
            host,
            user,
            port,
            password,
            autocommit=True,
        )
        actual_details = hsql.db_connection_to_tuple(self.connection)
        expected = {'dbname': dbname,
                    'host' : host,
                    'port' : port,
                    'user' : user,
                    'password' :password}
        self.assertEqual(actual_details._asdict(), expected) 

    @pytest.mark.slow()
    def test_create_database(self):
        """
        Verify that db is creating.
        """
        dbname = "im_postgres_db_local"
        host = "localhost"
        port = 5432
        password = "alsdkqoen"
        user = "aljsdalsd"
        hsql.wait_db_connection(dbname, port, host)
        self.connection, _ = hsql.get_connection(
            dbname,
            host,
            user,
            port,
            password,
            autocommit=True,
        )
        hsql.create_database(
            self.connection,
            db="test_db"
        )
        self.assertIn("test_db", hsql.get_db_names(self.connection))

