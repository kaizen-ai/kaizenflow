"""
Extract part of the ETL and QA pipeline.

Import as:

import sorrentum_sandbox.projects.Issue26_Team7_Implement_sandbox_for_Chainlink.download as sisebido
"""
import logging

import pandas as pd
from web3 import Web3
from tqdm import tqdm

import sorrentum_sandbox.common.download as ssandown

_LOG = logging.getLogger(__name__)

def downloader(**kwargs):
    web3 = Web3(Web3.HTTPProvider('https://rpc.ankr.com/eth'))
    abi = '[{"inputs":[],"name":"decimals","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"description","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint80","name":"_roundId","type":"uint80"}],"name":"getRoundData","outputs":[{"internalType":"uint80","name":"roundId","type":"uint80"},{"internalType":"int256","name":"answer","type":"int256"},{"internalType":"uint256","name":"startedAt","type":"uint256"},{"internalType":"uint256","name":"updatedAt","type":"uint256"},{"internalType":"uint80","name":"answeredInRound","type":"uint80"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"latestRoundData","outputs":[{"internalType":"uint80","name":"roundId","type":"uint80"},{"internalType":"int256","name":"answer","type":"int256"},{"internalType":"uint256","name":"startedAt","type":"uint256"},{"internalType":"uint256","name":"updatedAt","type":"uint256"},{"internalType":"uint80","name":"answeredInRound","type":"uint80"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"version","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"}]'
    addr = '0xaEA2808407B7319A31A383B6F8B60f04BCa23cE2'
    contract = web3.eth.contract(address=addr, abi=abi)
    latestData = contract.functions.latestRoundData().call()
    data_lst = []
    if 'roundid' in kwargs:
        roundid = kwargs['roundid']
        for i in tqdm(range(latestData[0], int(roundid), -1)):
            data = contract.functions.getRoundData(i).call()
            data_lst.append(data)
            data_df = pd.DataFrame(data_lst, columns = ['roundId', 'price', 'startedAt', 'updatedAt', 'answeredInRound'])

            
    elif 'num_of_data' in kwargs:
        num_of_data = kwargs['num_of_data']
        for i in tqdm(range(int(num_of_data))):
            data = contract.functions.getRoundData(latestData[0]-i).call()
            data_lst.append(data)
            data_df = pd.DataFrame(data_lst, columns = ['roundId', 'price', 'startedAt', 'updatedAt', 'answeredInRound'])
            
    data_df['pair'] = contract.functions.description().call()
    data_df['decimals'] = contract.functions.decimals().call()
    data_df['startedAt'] = pd.to_datetime(data_df['startedAt'],unit='s')
    data_df['updatedAt'] = pd.to_datetime(data_df['updatedAt'],unit='s')
        
    _LOG.info(f"Downloaded data: \n\t {data_df.head()}")    
    return ssandown.RawData(data_df)
