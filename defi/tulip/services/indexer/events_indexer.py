"""
Import as:

import defi.tulip.services.indexer.events_indexer as dtsievin
"""

import json
import logging
import os
import time
from typing import Any, Dict, List

import psycopg2 as psycop
import web3

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


def _get_contract(path_to_contract: str) -> web3.eth.Contract:
    """
    Instantiate the Tulip contract.
    """
    infura_key = os.environ.get("API_KEY")
    # Connect to the network.
    web3_provider = web3.Web3(
        web3.HTTPProvider(f"https://sepolia.infura.io/v3/{infura_key}")
    )
    tulip_address = os.environ.get("TULIP_ADDRESS")
    with open(path_to_contract, "r") as f:
        tulip_abi = json.load(f)
    # Instantiate the contract.
    contract = web3_provider.eth.contract(
        address=tulip_address, abi=tulip_abi["abi"]
    )
    return contract


def _add_order_to_db(
    event_type: str, arguments: Dict[str, Any], cursor: psycop.extensions.cursor
):
    """
    Put the values from the order event to the `tulip_orders` database.
    """
    query = """
        INSERT INTO public.tulip_orders
        (swap_pair_id, swap_id, order_id, order_direction, basetoken, querytoken,
        depositaddress, senderaddress, timestamp, amount, limitprice)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    # Transform "newBuyOrder" -> "buy" / "newSellOrder" -> "sell".
    event_type = event_type[3:-5].lower()
    # TODO(Toma): We don't need timestamp since we have knowledge_timestamp, remove the field later.
    timestamp = int(time.time())
    # Also probably we should save block number or transaction hash.
    values = (
        arguments["pairID"],
        arguments["swapID"],
        arguments["orderID"],
        event_type,
        arguments["baseToken"],
        arguments["quoteToken"],
        arguments["depositAddress"],
        arguments["senderAddress"],
        timestamp,
        arguments["amount"],
        arguments["limitPrice"],
    )
    # Add order to the DB.
    cursor.execute(query, values)


def _catch_events(
    db_connection: DbConnection,
    event_filters: List[web3._utils.filters.LogFilter],
    poll_interval: int,
):
    """
    Catch buy and sell orders and put them to the database.
    """
    cursor = db_connection.cursor()
    while True:
        for event_filter in event_filters:
            for event in event_filter.get_new_entries():
                event_type = event["event"]
                # TODO(Toma): We don't have a field in db for transaction hash yet,
                # but probably we should.
                # tx_hash = event["transactionHash"]
                arguments = event["args"]
                _add_order_to_db(event_type, arguments, cursor)
        time.sleep(poll_interval)


# #############################################################################


def main():
    # Get DB connection.
    db_connection = _get_connection_from_env_vars()
    # Get Tulip contact entity.
    contract = _get_contract("TulipABI.json")
    # Set-up filter for sell orders.
    print(web3.__version__)
    sell_event_filter = contract.events.newSellOrder.createFilter(
        fromBlock="latest"
    )
    # Set-up filter for buy orders.
    buy_event_filter = contract.events.newBuyOrder.createFilter(
        fromBlock="latest"
    )
    _catch_events(db_connection, [sell_event_filter, buy_event_filter], 2)


if __name__ == "__main__":
    main()
