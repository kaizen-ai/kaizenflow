import web3

import os
import time


INFURA_KEY = os.environ.get("API_KEY")
# Connect to the network.
web3 = web3.Web3(web3.HTTPProvider(f"https://sepolia.infura.io/v3/{INFURA_KEY}")) 
# The address that holds the contract
contract_address = "0xContractAddress"
# ABI is a JSON formatted list of contract methods. 
# build tulip contract from another branch and move ABI to this directory
abi = ""
# Instantiate the contract
contract = web3.eth.contract(address=contract_address, abi=abi)


def handle_event(event):
    print(event)  
    # Add events to the DB.

def log_loop(event_filter, poll_interval: int):
    while True:
        try:
            for event in event_filter.get_new_entries():
                handle_event(event)
            time.sleep(poll_interval)
        except Exception as e:
            print(f"Error getting events: {e}")
            # LOG ERROR!
            time.sleep(poll_interval)  

def main():
    event_filter = contract.events.YourEvent.createFilter(fromBlock='latest')
    log_loop(event_filter, 2)

if __name__ == "__main__":
    main()
