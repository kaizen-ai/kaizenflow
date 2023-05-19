import web3

import os
import time
from typing import List


INFURA_KEY = os.environ.get("API_KEY")
# The address that holds the contract
CONTRACT_ADDR = "0xContractAddress"
# ABI is a JSON formatted list of contract methods. 
with open("TulipABI.json", "r") as f:
    ABI = f.read()

def handle_event(event):
    """

    """
    print(event)  
    # Add events to the DB.

def log_loop(event_filters: List, poll_interval: int):
    """

    """
    while True:
        try:
            for event_filter in event_filters:
                for event in event_filter.get_new_entries():
                    handle_event(event)
                time.sleep(poll_interval)
        except Exception as e:
            print(f"Error getting events: {e}")
            # LOG ERROR!
            time.sleep(poll_interval)  

def main():
    """
    """
    # Connect to the network.
    web3 = web3.Web3(web3.HTTPProvider(f"https://sepolia.infura.io/v3/{INFURA_KEY}")) 
    # Instantiate the contract.
    contract = web3.eth.contract(address=CONTRACT_ADDR, abi=ABI)
    # Set-up filter for sell orders.
    sell_event_filter = contract.events.newSellOrder.createFilter(fromBlock='latest')
    # Set-up filter for buy orders.
    buy_event_filter = contract.events.newBuyOrder.createFilter(fromBlock='latest')
    log_loop([sell_event_filter, buy_event_filter], 2)

if __name__ == "__main__":
    main()
