import logging
import os

import pandas as pd

import helpers.printing as hprint
import helpers.sql as hsql
import helpers.system_interaction as hsysinte
import helpers.unit_test as hunitest
import oms.oms_db as oomsdb

_LOG = logging.getLogger(__name__)


# TODO(gp): Generalize and move this to hsql.py or hsql_test.py.
class _TestOmsDbHelper(hunitest.TestCase):
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
        dbname = "oms_postgres_db_local"
        host = "localhost"
        port = 5432
        password = "alsdkqoen"
        user = "aljsdalsd"
        self.dbname = dbname
        conn_exists = hsql.check_db_connection(host, dbname, port)
        if conn_exists:
            _LOG.warning("DB is already up: skipping docker compose")
            # Since we have found the DB already up, we assume that we need to
            # leave it running after the tests
            self.bring_down_db = False
        else:
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
            # Wait for the DB to be available.
            hsql.wait_db_connection(dbname, port, host)
            self.bring_down_db = True
        # Save connection info.
        self.connection, self.cursor = hsql.get_connection(
            self.dbname,
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
        if self.bring_down_db:
            cmd = (
                "sudo docker-compose "
                f"--file {self.docker_compose_file_path} down -v"
            )
            hsysinte.system(cmd, suppress_output=False)
        else:
            _LOG.warning("Leaving DB up")
        super().tearDown()


# #############################################################################


class TestOmsDb1(_TestOmsDbHelper):
#class TestOmsDb1(hunitest.TestCase):

    # def _get_connection(self):
    #     try:
    #         return self.connection
    #     except AttributeError:
    #         dbname = "oms_postgres_db_local"
    #         host = "localhost"
    #         port = 5432
    #         password = "alsdkqoen"
    #         user = "aljsdalsd"
    #         # Wait for the DB to be available.
    #         hsql.wait_db_connection(dbname, port, host)
    #         # Save connection info.
    #         self.connection, cursor = hsql.get_connection(
    #             dbname,
    #             host,
    #             user,
    #             port,
    #             password,
    #             autocommit=True,
    #         )
    #         return self.connection
    #

    def test_up1(self) -> None:
        """
        Verify that the DB is up.
        """
        db_list = hsql.get_db_names(self.connection)
        _LOG.info("db_list=%s", db_list)

    def test_create_table1(self) -> None:
        # Clean up the table.
        table_name = "target_files_processed_candidate_view"
        hsql.remove_table(self.connection, table_name)
        #
        db_tables = hsql.get_table_names(self.connection)
        _LOG.info("get_table_names=%s", db_tables)
        self.assertEqual(db_tables, [])
        # Create the table.
        query = oomsdb.get_create_target_files_table_query(incremental=False)
        _LOG.debug("query=%s", query)
        self.connection.cursor().execute(query)
        #
        db_tables = hsql.get_table_names(self.connection)
        _LOG.info("get_table_names=%s", db_tables)
        self.assertEqual(db_tables, ["target_files_processed_candidate_view"])

    @staticmethod
    def _to_series(txt) -> pd.Series:
        _LOG.debug("txt=\n%s", txt)
        tuples = [tuple(line.split("|")) for line in hprint.dedent(txt).split("\n")]
        _LOG.debug("tuples=%s", str(tuples))
        # Remove empty tuples.
        tuples = [t for t in tuples if t[0] != ""]
        index, data = zip(*tuples)
        _LOG.debug("index=%s", index)
        _LOG.debug("data=%s", data)
        srs = pd.Series(data, index=index)
        return srs

    def test_insert1(self) -> None:
        # Create the table.
        query = oomsdb.get_create_target_files_table_query(incremental=True)
        _LOG.debug("query=%s", query)
        self.connection.cursor().execute(query)
        #
        row1 = """
        tradedate|2021-11-12
        targetlistid|1
        instanceid|3504
        filename|s3://targets/20211112000000/test.csvtest.csv
        strategyid|SAU1
        timestamp_processed|2021-11-12 19:59:23.710677
        timestamp_db|2021-11-12 19:59:23.716732
        target_count|1
        changed_count|0
        unchanged_count|0
        cancel_count|0
        success|False
        reason|"There were a total of 1 malformed requests in the file.
        """
        srs = self._to_series(row1)
        table_name = "target_files_processed_candidate_view"
        hsql.execute_insert_query(self.connection, srs, table_name)
        #
        query = f"SELECT * FROM {table_name}"
        df = hsql.execute_query(self.connection, query)
        act = hprint.dataframe_to_str(df)
        exp = ""
        self.assert_equal(act, exp)
        #
        # row2 = """
        # tradedate,2021-11-12
        # targetlistid,2
        # instanceid,3504
        # filename,s3://targets/20211112000000/positions.16.2021-11-12_15:44:04.554833-05:00.csv
        # strategyid,SAU1
        # timestamp_processed,2021-11-12 20:45:07.463641
        # timestamp_db,2021-11-12 20:45:07.469807
        # target_count,1
        # changed_count,0
        # unchanged_count,0
        # cancel_count,0
        # success,False
        # reason,"There were a total of 1 malformed requests in the file.  First 5 malformed entries were ([Failed to process OrderedDict([('trade_date', '20211112'), ('egid', '15151'), ('target_position', '5'), ('notional_limit', '477.32599999999996'), ('adjprice', '94.52'), ('adjprice_ccy', 'USD'), ('algo', 'VWAP'), ('STARTTIME', '2021-11-12 15:44:04.554833-05:00'), ('ENDTIME', '2021-11-12 15:49:04.554833-05:00'), ('MAXPCTVOL', '3')]) due to 'CURRENT_POSITION'])"
        # """
        #
        # row3 = """
        # tradedate,2021-11-12
        # targetlistid,5
        # instanceid,3504
        # filename,s3://targets/20211112000000/positions.3.2021-11-12_16:38:22.906790-05:00.csv
        # strategyid,SAU1
        # timestamp_processed,2021-11-12 21:38:39.414138
        # timestamp_db,2021-11-12 21:38:39.419536
        # target_count,1
        # changed_count,1
        # unchanged_count,0
        # cancel_count,0
        # success,True
        # reason,
        # """

    def test_wait_for_table1(self):
        pass
