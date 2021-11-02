import logging
import helpers.sql as hsql
import helpers.system_interaction as hsyint
import im.common.db.utils as imcodbuti
import im.common.db.create_db as imcodbcrdb
import helpers.unit_test as huntes

_LOG = logging.getLogger(__name__)

class Test1(huntes.TestCase):

    def setUp(self):
        """
        Bring up the DB.
        """
        cmd = "cd im/devops; sudo docker-compose --file compose/docker-compose.yml up -d im_postgres_local"
        hsyint.system(cmd, suppress_output=False)

    def tearDown(self):
        """
        Bring up the DB.
        """
        cmd = "cd im/devops; sudo docker-compose --file compose/docker-compose.yml down -v"
        hsyint.system(cmd, suppress_output=False)

    def test_create_all_tables1(self):
        #
        dbname = "im_postgres_db_local"
        host = "localhost"
        port = 5432
        user = "aljsdalsd"
        password = "alsdkqoen"
        # Create a connection to the local DB which runs on localhost on a certain port.
        connection, _ = hsql.get_connection( dbname, host, user, port,
                password, autocommit=True,)
        _LOG.info("Waiting...")
        imcodbuti.check_db_connection(connection)
        _LOG.info("done...")
        _LOG.info("before")
        _LOG.info(hsql.get_db_names(connection))
        _LOG.info(hsql.get_table_names(connection))
        # Check that everything is empty.
        # docker-compose down doesn't clean up the db perfectly because the volume is left.
        #
        imcodbcrdb.create_all_tables(connection)
        #
        _LOG.info("after")
        _LOG.info(hsql.get_db_names(connection))
        _LOG.info(hsql.get_table_names(connection))
        assert 0
