import asyncio
import logging
import os

import pandas as pd

import helpers.datetime_ as hdateti
import helpers.hasyncio as hasynci
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

def _to_series(txt) -> pd.Series:
    #_LOG.debug("txt=\n%s", txt)
    tuples = [
        tuple(line.split("|")) for line in hprint.dedent(txt).split("\n")
    ]
    #_LOG.debug("tuples=%s", str(tuples))
    # Remove empty tuples.
    tuples = [t for t in tuples if t[0] != ""]
    index, data = zip(*tuples)
    #_LOG.debug("index=%s", index)
    #_LOG.debug("data=%s", data)
    srs = pd.Series(data, index=index)
    return srs

def _get_row1() -> pd.Series:
    row = """
    tradedate|2021-11-12
    targetlistid|1
    instanceid|3504
    filename|hello_world.txt
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
    srs = _to_series(row)
    return srs

def _get_row2() -> pd.Series:
    row = """
    tradedate|2021-11-12
    targetlistid|2
    instanceid|3504
    filename|s3://targets/20211112000000/positions.16.2021-11-12_15:44:04-05:00.csv
    strategyid|SAU1
    timestamp_processed|2021-11-12 20:45:07.463641
    timestamp_db|2021-11-12 20:45:07.469807
    target_count|1
    changed_count|0
    unchanged_count|0
    cancel_count|0
    success|False
    reason|"There were a total of 1 malformed requests in the file."
    """
    srs = _to_series(row)
    return srs

def _get_row3() -> pd.Series:
    row = """
    tradedate|2021-11-12
    targetlistid|5
    instanceid|3504
    filename|s3://targets/20211112000000/positions.3.2021-11-12_16:38:22-05:00.csv
    strategyid|SAU1
    timestamp_processed|2021-11-12 21:38:39.414138
    timestamp_db|2021-11-12 21:38:39.419536
    target_count|1
    changed_count|1
    unchanged_count|0
    cancel_count|0
    success|True
    reason|
    """
    srs = _to_series(row)
    return srs


class TestOmsDb1(_TestOmsDbHelper):
    def test_up1(self) -> None:
        """
        Verify that the DB is up.
        """
        db_list = hsql.get_db_names(self.connection)
        _LOG.info("db_list=%s", db_list)

    def test_create_table1(self) -> None:
        """
        Test creating the table.
        """
        # Clean up the table.
        table_name = "target_files_processed_candidate_view"
        hsql.remove_table(self.connection, table_name)
        # The DB should have no tables.
        db_tables = hsql.get_table_names(self.connection)
        _LOG.info("get_table_names=%s", db_tables)
        self.assertEqual(db_tables, [])
        # Create the table.
        _ = oomsdb.create_target_files_table(self.connection, incremental=False)
        # The table should be present.
        db_tables = hsql.get_table_names(self.connection)
        _LOG.info("get_table_names=%s", db_tables)
        self.assertEqual(db_tables, ["target_files_processed_candidate_view"])

    def test_insert1(self) -> None:
        """
        Test inserting in the table.
        """
        # Create the table.
        table_name = oomsdb.create_target_files_table(self.connection, incremental=True)
        # Insert a row.
        row = _get_row1()
        hsql.execute_insert_query(self.connection, row, table_name)
        # Insert another row.
        row = _get_row2()
        hsql.execute_insert_query(self.connection, row, table_name)
        # Insert another row.
        row = _get_row3()
        hsql.execute_insert_query(self.connection, row, table_name)
        # Check the content of the table.
        query = f"SELECT * FROM {table_name}"
        df = hsql.execute_query(self.connection, query)
        act = hprint.dataframe_to_str(df)
        exp = r"""   
           targetlistid   tradedate  instanceid                                                                filename strategyid        timestamp_processed               timestamp_db  target_count  changed_count  unchanged_count  cancel_count  success                                                     reason
        0             1  2021-11-12        3504                                                         hello_world.txt       SAU1 2021-11-12 19:59:23.710677 2021-11-12 19:59:23.716732             1              0                0             0    False   "There were a total of 1 malformed requests in the file.
        1             2  2021-11-12        3504  s3://targets/20211112000000/positions.16.2021-11-12_15:44:04-05:00.csv       SAU1 2021-11-12 20:45:07.463641 2021-11-12 20:45:07.469807             1              0                0             0    False  "There were a total of 1 malformed requests in the file."
        2             5  2021-11-12        3504   s3://targets/20211112000000/positions.3.2021-11-12_16:38:22-05:00.csv       SAU1 2021-11-12 21:38:39.414138 2021-11-12 21:38:39.419536             1              1                0             0     True                                                           """
        self.assert_equal(act, exp, fuzzy_match=True)


class TestOmsDb2(_TestOmsDbHelper):

    def wait_for_table_helper(self, coroutines):
        with hasynci.solipsism_context() as event_loop:
            get_wall_clock_time = lambda: hdateti.get_current_time(
                tz="ET", event_loop=event_loop
            )
            async def _workload(*coroutines):
                result = await asyncio.gather(*coroutines)
                return result

            # Construct the coroutines here by passing the `get_wall_clock_time`
            # function.
            coroutines = [coro(get_wall_clock_time) for coro in coroutines]
            # Run.
            coroutine = _workload(*coroutines)
            res = hasynci.run(coroutine, event_loop=event_loop)
            return res

    async def _db_poller(self, get_wall_clock_time):
        target_value = "hello_world.txt"
        poll_kwargs = {
            "sleep_in_secs": 1.0,
            "timeout_in_secs": 5.0,
            "get_wall_clock_time": get_wall_clock_time,
        }
        _LOG.debug("get_wall_clock_time=%s", get_wall_clock_time())
        coro = oomsdb.wait_for_target_ack(
            self.connection, target_value, poll_kwargs
        )
        _LOG.debug("get_wall_clock_time=%s", get_wall_clock_time())
        result = await asyncio.gather(coro)
        return result

    async def _db_writer(self, sleep_in_secs, table_name, get_wall_clock_time) -> None:
        # Sleep.
        _LOG.debug("get_wall_clock_time=%s", get_wall_clock_time())
        _LOG.debug("sleep for %s secs", sleep_in_secs)
        await asyncio.sleep(sleep_in_secs)
        # Insert the row.
        _LOG.debug("get_wall_clock_time=%s", get_wall_clock_time())
        _LOG.debug("insert ...")
        row = _get_row1()
        hsql.execute_insert_query(self.connection, row, table_name)
        _LOG.debug("get_wall_clock_time=%s", get_wall_clock_time())
        _LOG.debug("insert ... done")
        # Show the state of the DB.
        query = f"SELECT * FROM {table_name}"
        df = hsql.execute_query(self.connection, query)
        _LOG.debug("df=\n%s", hprint.dataframe_to_str(df, use_tabulate=True))

    def test_wait_for_table1(self):
        """
        Show that if the value doesn't show up in the DB there is a timeout.
        """
        # Create only one coroutine waiting for a row in the table that never comes,
        # causing a timeout.
        coroutines = [self._db_poller]
        with self.assertRaises(TimeoutError):
            self.wait_for_table_helper(coroutines)

    def test_wait_for_table2(self):
        """
        Show that waiting on a value on the table works.
        """
        table_name = oomsdb.create_target_files_table(self.connection, incremental=True)
        coroutines = []
        # Add a DB poller waiting for a row in the table.
        coroutines.append(self._db_poller)
        # Add a DB writer that will write after 2 seconds, making the DB poller
        # exiting successfully.
        sleep_in_secs = 2
        coroutines.append(lambda gwct: self._db_writer(sleep_in_secs, table_name, gwct))
        # Run.
        res = self.wait_for_table_helper(coroutines)
        # Check output.
        act = str(res)
        # The output is (DB poller, DB writer).
        exp = r"""[[(True, 1)], None]"""
        self.assert_equal(act, exp)

    def test_wait_for_table3(self):
        """
        The data is written too late triggering a timeout.
        """
        table_name = oomsdb.create_target_files_table(self.connection, incremental=True)
        coroutines = []
        # Add a DB poller waiting for a row in the table.
        coroutines.append(self._db_poller)
        # Add a DB writer that will write after 10 seconds, after the DB poller ends
        # after 5 secs.
        sleep_in_secs = 10
        coroutines.append(lambda gwct: self._db_writer(sleep_in_secs, table_name, gwct))
        # Run.
        with self.assertRaises(TimeoutError):
            self.wait_for_table_helper(coroutines)