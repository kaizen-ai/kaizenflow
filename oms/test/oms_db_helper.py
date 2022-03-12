import logging
from typing import Any, Callable

import helpers.hsql as hsql
import helpers.hsql_test as hsqltest
import oms.oms_lib_tasks as oomlitas

_LOG = logging.getLogger(__name__)


class TestOmsDbHelper(hsqltest.TestDbHelper):
    """
    Configure the helper to build an OMS test DB.
    """

    def _test_create_table_helper(
        self: Any,
        table_name: str,
        create_table_func: Callable,
    ) -> None:
        """
        Run sanity check for a DB table.

        - Test that the DB is up
        - Remove the table `table_name`
        - Create the table `table_name` using `create_table_func()`
        - Check that the table exists
        - Delete the table
        """
        # Verify that the DB is up.
        db_list = hsql.get_db_names(self.connection)
        _LOG.info("db_list=%s", db_list)
        # Clean up the table.
        hsql.remove_table(self.connection, table_name)
        # The DB should not have this table.
        db_tables = hsql.get_table_names(self.connection)
        _LOG.info("get_table_names=%s", db_tables)
        self.assertNotIn(table_name, db_tables)
        # Create the table.
        _ = create_table_func(self.connection, incremental=False)
        # The table should be present.
        db_tables = hsql.get_table_names(self.connection)
        _LOG.info("get_table_names=%s", db_tables)
        self.assertIn(table_name, db_tables)
        # Delete the table.
        hsql.remove_table(self.connection, table_name)

    @staticmethod
    def _get_compose_file() -> str:
        return "oms/devops/compose/docker-compose.yml"

    @staticmethod
    def _get_service_name() -> str:
        return "oms_postgres"

    @staticmethod
    def _get_db_env_path() -> str:
        """
        See `_get_db_env_path()` in the parent class.
        """
        # Use the `local` stage for testing.
        env_file_path = oomlitas.get_db_env_path("local")
        return env_file_path  # type: ignore[no-any-return]