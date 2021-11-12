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
        self.dbname = "im_postgres_db_local"
        self.host = "localhost"
        self.port = 5432
        self.password = "alsdkqoen"
        self.user = "aljsdalsd"
        self.connection, _ = hsql.get_connection(
            self.dbname,
            self.host,
            self.user,
            self.port,
            self.password,
            autocommit=True,
        )
        
    def tearDown(self):
        """
        Bring down the database inside the test container.
        """
        cmd = ("sudo docker-compose "
               f"--file {self.docker_compose_file_path} down -v")
        hsyint.system(cmd, suppress_output=False)

        super().tearDown()

    def test_checkdb(self) -> None:
        """
        Smoke test.
        """
        hsql.check_db_connection(self.connection)

    def test_db_connection_to_str(self) -> None:
        """
        Verify that connection string is correct.
        """
        hsql.check_db_connection(self.connection)
        actual_str = hsql.db_connection_to_str(self.connection)
        expected = (f"dbname={self.dbname}\n"
                    f"host={self.host}\n"
                    f"port={self.port}\n"
                    f"user={self.user}\n"
                    f"password={self.password}")
        self.assertEqual(actual_str, expected) 

    def test_create_database(self):
        """
        Verify that db is created.
        """
        hsql.check_db_connection(self.connection)
        hsql.create_database(
            self.connection,
            db="test_db"
        )
        self.assertIn("test_db", hsql.get_db_names(self.connection))

