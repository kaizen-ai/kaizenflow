import abc
import logging
import os
from typing import Any, Callable

import helpers.hgit as hgit
import helpers.hio as hio
import helpers.hsql as hsql
import helpers.hsql_test as hsqltest
import oms.oms_lib_tasks as oomlitas

_LOG = logging.getLogger(__name__)


class TestOmsDbHelper(hsqltest.TestDbHelper, abc.ABC):
    """
    Configure the helper to build an OMS test DB.
    """

    # TODO(gp): For some reason without having this function defined, the
    # derived classes can't be instantiated because of get_id().
    @classmethod
    @abc.abstractmethod
    def get_id(cls) -> int:
        raise NotImplementedError

    @classmethod
    def _get_compose_file(cls) -> str:
        idx = cls.get_id()
        dir_name = hgit.get_amp_abs_path()
        docker_compose_path = os.path.join(
            dir_name, "oms/devops/compose/docker-compose.yml"
        )
        docker_compose_path_idx: str = hio.add_idx_to_filename(
            docker_compose_path, idx
        )
        return docker_compose_path_idx

    @classmethod
    def _get_service_name(cls) -> str:
        idx = cls.get_id()
        return "oms_postgres" + str(idx)

    # TODO(gp): Use file or path consistently.
    @classmethod
    def _get_db_env_path(cls) -> str:
        """
        See `_get_db_env_path()` in the parent class.
        """
        # Use the `local` stage for testing.
        idx = cls.get_id()
        env_file_path = oomlitas.get_db_env_path("local", idx=idx)
        return env_file_path  # type: ignore[no-any-return]

    @classmethod
    def _create_docker_files(cls) -> None:
        service_name = cls._get_service_name()
        idx = cls.get_id()
        host_port = 5432 + idx
        txt = f"""version: '3.5'

services:
  # Docker container running Postgres DB.
  {service_name}:
    image: postgres:13
    restart: "no"
    environment:
      - POSTGRES_HOST=${{POSTGRES_HOST}}
      - POSTGRES_DB=${{POSTGRES_DB}}
      - POSTGRES_PORT={{POSTGRES_PORT}}
      - POSTGRES_USER=${{POSTGRES_USER}}
      - POSTGRES_PASSWORD=${{POSTGRES_PASSWORD}}
    volumes:
      - {service_name}_data:/var/lib/postgresql/data
    ports:
      - {host_port}:5432

volumes:
  {service_name}_data: {{}}

networks:
  default:
    name: {service_name}_network
"""
        compose_file_name = cls._get_compose_file()
        hio.to_file(compose_file_name, txt)
        #
        txt = f"""POSTGRES_HOST=localhost
POSTGRES_DB=oms_postgres_db_local
POSTGRES_PORT={host_port}
POSTGRES_USER=aljsdalsd
POSTGRES_PASSWORD=alsdkqoen"""
        env_file_name = cls._get_db_env_path()
        hio.to_file(env_file_name, txt)

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
