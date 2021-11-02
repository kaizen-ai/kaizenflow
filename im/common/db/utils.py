"""
Import as:

import im.common.db.utils as imcodbuti
"""

import logging
import os
import time

import helpers.sql as hsql
import helpers.system_interaction as hsyint

_LOG = logging.getLogger(__name__)


# TODO(gp): -> It should go to hsql.
def check_db_connection(connection: hsql.DbConnection) -> None:
    """
    Verify that the database is available.

    :param connection: a database connection
    """
    _LOG.info(
        "Checking the database connection:\n%s",
        db_connection_to_str(connection=connection),
    )
    while True:
        _LOG.info("Waiting for PostgreSQL to become available...")
        cmd = "pg_isready -d %s -p %s -h %s"
        rc = hsyint.system(
            cmd
            % (
                connection.info.dbname,
                connection.info.port,
                connection.info.host,
            ),
            abort_on_error=False,
        )
        time.sleep(1)
        if rc == 0:
            _LOG.info("PostgreSQL is available")
            break


# TODO(gp): -> It should go to hsql.
def db_connection_to_str(connection: hsql.DbConnection) -> str:
    """
    Get database connection details using environment variables. Connection
    details include:

        - Database name
        - Host
        - Port
        - Username
        - Password

    :param connection: a database connection
    :return: database connection details
    """
    info = connection.info
    txt = []
    txt.append("dbname='%s'" % info.dbname)
    txt.append("host='%s'" % info.host)
    txt.append("port='%s'" % info.port)
    txt.append("user='%s'" % info.user)
    txt.append("password='%s'" % info.password)
    txt = "\n".join(txt)
    return txt


def is_inside_im_container() -> bool:
    """
    Return whether we are running inside IM app.

    :return: True if running inside the IM app, False otherwise
    """
    # TODO(*): Why not testing only STAGE?
    condition = (
        os.environ.get("STAGE") == "TEST"
        and os.environ.get("POSTGRES_HOST") == "im_postgres_test"
    ) or (
        os.environ.get("STAGE") == "LOCAL"
        and os.environ.get("POSTGRES_HOST") == "im_postgres_local"
    )
    return condition
