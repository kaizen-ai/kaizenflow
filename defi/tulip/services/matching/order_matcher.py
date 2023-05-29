"""
Import as:

import defi.tulip.services.matching.order_matcher as dtsmorma
"""

import logging
import os
from typing import Any, List

import psycopg2 as psycop

import defi.tulip.implementation.order as dtuimord
import defi.tulip.implementation.order_matching as dtimorma

_LOG = logging.getLogger(__name__)

DbConnection = Any


def _get_connection_from_env_vars(autocommit: bool = True) -> DbConnection:
    """
    Create a SQL connection with the information from the environment
    variables.
    """
    # Get values from the environment variables.
    host = os.environ["POSTGRES_HOST"]
    dbname = os.environ["POSTGRES_DB"]
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    _LOG.debug(f"{host} {dbname} {user}")
    # Build the connection.
    connection = psycop.connect(
        host=host, dbname=dbname, user=user, password=password
    )
    if autocommit:
        connection.autocommit = True
    return connection


def _extract_matching_orders(
    swap_id: int, connection: DbConnection
) -> List[dtuimord.Order]:
    """
    Get orders with a specified swap id from the database.

    `tulip_orders` DB has the following schema:
    ```
    0  |  id                  | bigint
    1  |  swap_pair_id        | integer
    2  |  swap_id             | bigint
    3  |  order_id            | bigint
    4  |  order_direction     | character varying(255)
    5  |  basetoken           | character varying(255)
    6  |  querytoken          | character varying(255)
    7  |  depositaddress      | character varying(255)
    8  |  senderaddress       | character varying(255)
    9  |  timestamp           | bigint
    10 |  amount              | numeric
    11 |  limitprice          | numeric
    12 |  knowledge_timestamp | timestamp with time zone
    ```

    :param swap_id: swap id to match the orders by
    :param connection: database connection
    :return: all the matched orders
    """
    # Execute the query to fetch all the matching orders.
    query = f"SELECT * FROM public.tulip_orders WHERE swap_id = {swap_id}"
    with connection.cursor() as cursor:
        cursor.execute(query)
        matching_db_orders = cursor.fetchall()
    # Covert DB orders to the conventional format.
    orders = []
    for db_order in matching_db_orders:
        order = dtuimord.Order(
            db_order[9],
            db_order[4],
            db_order[10],
            db_order[5],
            db_order[11],
            db_order[6],
            db_order[7],
            db_order[8],
        )
        orders.append(order)
    return orders


# #############################################################################


def main() -> None:
    # Get DB connection.
    db_connection = _get_connection_from_env_vars()
    # Get global parameters.
    swap_id = os.environ.get("SWAP_ID")
    clearing_price = os.environ.get("CLEARING_PRICE")
    base_token = os.environ.get("BASE_TOKEN")
    quote_token = os.environ.get("QUOTE_TOKEN")
    # Get matching orders from DB.
    matching_orders = _extract_matching_orders(swap_id, db_connection)
    # Match orders and get implemented transfers.
    transfer_df = dtimorma.match_orders(
        matching_orders, clearing_price, base_token, quote_token
    )
    # TODO(Dan): Add next steps.


if __name__ == "__main__":
    main()
