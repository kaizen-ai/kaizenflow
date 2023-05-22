import psycopg2 as psycop
import web3

import logging
import os
import time
from typing import Any, Dict, List


_LOG = logging.getLogger(__name__)

DbConnection = Any


def get_connection_from_env_vars(autocommit: bool = True) -> DbConnection:
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


def get_contract() -> web3.eth.Contract:
    """
    Instantiate the Tulip contract.
    """
    infura_key = os.environ.get("API_KEY")
    # Connect to the network.
    web3 = web3.Web3(web3.HTTPProvider(f"https://sepolia.infura.io/v3/{infura_key}")) 
    tulip_address = os.environ.get("TULIP_ADDRESS")
    with open("TulipABI.json", "r") as f:
        tulip_abi = f.read()
    # Instantiate the contract.
    contract = web3.eth.contract(address=tulip_address, abi=tulip_abi)
    return contract

def send_event_to_db(event_type: str, arguments: Dict[str, Any], cursor: psycop.extensions.cursor):
    """
    """
    
    # Add events to the DB.

def log_loop(db_connection: DbConnection, event_filters: List[web3._utils.filters.LogFilter], poll_interval: int):
    """
    Catch buy and sell orders and put them to the database.
    """
    cursor = db_connection.cursor()
    while True:
        for event_filter in event_filters:
            for event in event_filter.get_new_entries():
                # Buy or Sell.
                event_type = event["event"]
                #
                arguments = event["args"]
                send_event_to_db(event_type, arguments, cursor)
            time.sleep(poll_interval)
        time.sleep(poll_interval)  

def main():
    """
    """
    # Get DB connection.
    db_connection = get_connection_from_env_vars()
    # Get Tulip contact entity.
    contract = get_contract()
    # Set-up filter for sell orders.
    sell_event_filter = contract.events.newSellOrder.createFilter(fromBlock='latest')
    # Set-up filter for buy orders.
    buy_event_filter = contract.events.newBuyOrder.createFilter(fromBlock='latest')
    log_loop(db_connection, [sell_event_filter, buy_event_filter], 2)

if __name__ == "__main__":
    main()
