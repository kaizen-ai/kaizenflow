"""
Import as:

import im.common.db.utils as imcodbuti
"""

import logging
import time

import helpers.system_interaction as hsyint

_LOG = logging.getLogger(__name__)


def check_db_connection(
    db_name: str,
    host: str,
    user: str,
    port: int,
    password: str,
) -> None:
    """
    Verify that the database is available.

    :param db_name: name of database to connect to, e.g. `im_db_local`
    :param host: host name to connect to db
    :param user: user name to connect to db
    :param port: port to connect to db
    :param password: password to connect to db
    """
    _LOG.info(
        "Checking the database connection:\n%s",
        db_connection_to_str(
            db_name=db_name, host=host, user=user, port=port, password=password
        ),
    )
    while True:
        _LOG.info("Waiting for PostgreSQL to become available...")
        cmd = "pg_isready -d %s -p %s -h %s"
        rc = hsyint.system(
            cmd
            % (
                db_name,
                port,
                host,
            )
        )
        time.sleep(1)
        if rc == 0:
            _LOG.info("PostgreSQL is available")
            break


def db_connection_to_str(
    db_name: str, host: str, user: str, port: int, password: str
) -> str:
    """
    Get database connection details using environment variables. Connection
    details include:

        - Database name
        - Host
        - Port
        - Username
        - Password
    :param db_name: name of database to connect to, e.g. `im_db_local`
    :param host: host name to connect to db
    :param user: user name to connect to db
    :param port: port to connect to db
    :param password: password to connect to db
    :return: database connection details
    """
    txt = []
    txt.append("dbname='%s'" % db_name)
    txt.append("host='%s'" % host)
    txt.append("port='%s'" % port)
    txt.append("user='%s'" % user)
    txt.append("password='%s'" % password)
    txt = "\n".join(txt)
    return txt
