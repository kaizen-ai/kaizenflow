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
        
        super().setUp()
        dbname = "im_postgres_db_local"
        host = "localhost"
        port = 5432
        user = "aljsdalsd"
        password = "alsdkqoen"
        cmd = "sudo docker-compose --file im/devops/compose/docker-compose.yml up -d im_postgres_local"
        hsyint.system(cmd, suppress_output=False)
        self.connection, _ = hsql.get_connection( dbname, host, user, port,
                password, autocommit=True,)


    def tearDown(self):
        """
        Bring up the DB.
        """
        self.connection.close()
        cmd = "sudo docker-compose --file im/devops/compose/docker-compose.yml down -v"
        hsyint.system(cmd, suppress_output=False)
        super().tearDown()

    def test_create_all_tables1(self):
#        cmd = "sudo docker-compose --file im/devops/compose/docker-compose.yml up -d im_postgres_local"
#        hsyint.system(cmd, suppress_output=False)

        dbname = "im_postgres_db_local"
        host = "localhost"
        port = 5432
        user = "aljsdalsd"
        password = "alsdkqoen"
        # Create a connection to the local DB which runs on localhost on a certain port.

      #  connection, _ = hsql.get_connection( dbname, host, user, port,
      #          password, autocommit=True,)
        _LOG.info("Waiting...")
        imcodbuti.check_db_connection(self.connection)
        _LOG.info("done...")
        _LOG.info("before")
        _LOG.info(hsql.get_db_names(self.connection))
        _LOG.info(hsql.get_table_names(self.connection))
        # Check that everything is empty.
#        cmd = "sudo docker-compose --file im/devops/compose/docker-compose.yml down -v"
#        hsyint.system(cmd, suppress_output=False)

        # docker-compose down doesn't clean up the db perfectly because the volume is left.
        #
        imcodbcrdb.create_all_tables(self.connection)
        #
        _LOG.info("after")
        _LOG.info(hsql.get_db_names(self.connection))
        _LOG.info(hsql.get_table_names(self.connection))
        assert 0
