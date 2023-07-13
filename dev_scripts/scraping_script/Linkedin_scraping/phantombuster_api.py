# IMPORTS:
from pydantic import BaseModel
from tqdm import tqdm
import pandas as pd
import pyinputplus as pyip
import datetime
import json
import requests
import utils
import config
# from google_api import get_csv_for_phantombuster

# Getting API Key from config.py
API = config.PHANTOMBUSTER_API

###############################################################################
# Agent API
###############################################################################
def get_all_agents():
    """
    Get all agents from Phantombuster.
    URL: https://api.phantombuster.com/api/v1/user
    """
    url = "https://api.phantombuster.com/api/v1/user"
    headers = {"accept": "application/json", "X-Phantombuster-Key-1": API}
    request = requests.get(url, headers=headers, timeout=30)
    data = request.json()["data"]["agents"]
    data = pd.DataFrame(data)
    return data


def get_agent_id_by_keyword(keyword=None):
    """
    Get agent id by keyword, the most recent one Found.
    If no keyword, return the most recent agent.
    Ignore case.
    """
    data = get_all_agents()
    keyword = keyword.lower() if keyword else None
    if keyword:
        data = data[data["name"].str.lower().str.contains(keyword)]
        if data.empty:
            print(f"No agent found with keyword [ {keyword} ]")
            return None
        agent_id = data.iloc[-1]["id"]
    else:
        agent_id = data.iloc[-1]["id"]
    return agent_id


def get_agent_name_by_id(agent_id: str):
    """
    Get agent name by id.
    """
    data = get_agent_info(agent_id)
    data = json.loads(data)
    agent_name = data["name"]
    return agent_name


def get_agent_info(agent_id: str):
    """
    https://api.phantombuster.com/api/v2/agents/fetch
    """
    url = f"https://api.phantombuster.com/api/v2/agents/fetch?id={agent_id}"
    headers = {"accept": "application/json", "X-Phantombuster-Key-1": API}
    request = requests.get(url, headers=headers, timeout=30)
    data = request.json()
    data = json.dumps(data, indent=4)
    return data

###############################################################################
# Container API
###############################################################################
def get_containers_id_by_agent_id(agent_id: str, depth: int = 1):
    """
    Get containers id by agent id and depth.
    """
    url = f"https://api.phantombuster.com/api/v2/containers/fetch-all?agentId={agent_id}"
    headers = {"accept": "application/json", "X-Phantombuster-Key-1": API}
    response = requests.get(url, headers=headers, timeout=30)
    response = response.json()
    data = response["containers"]
    containers_id = []
    for container in range(len(data)):
        containers_id.append(data[container]["id"])
    return containers_id


def get_result_csv_by_container_id(container_id: str):
    """
    Get result csv by container id.
    """
    url = f"https://api.phantombuster.com/api/v2/containers/fetch-result-object?id={container_id}"
    headers = {"accept": "application/json", "X-Phantombuster-Key-1": API}
    response = requests.get(url, headers=headers, timeout=30)
    return response.text

# TODO(Yiyun): Modify to just get the most recent csv; Also, add convert result
# object function for Linkedin Search Scraper.
def get_result_csv(agent_id: str, depth: int = 1):
    containers_id = get_containers_id_by_agent_id(agent_id, depth)
    result_csvs = []
    response_array = []
    for container_id in tqdm(containers_id):
        response = get_result_csv_by_container_id(container_id)
        # print(response)
        if 'csv' in response: 
            # print(container_id)
            response_array += response.split('"')
        
    # finds url in split response array
    if response_array:
        for i in response_array:
            if ".csv" in i:
                csv_url = i.replace('\\', '') 
                result_csvs.append(csv_url)
                print("\nFetch result csv succesfully!")
    else:
        if depth >= 10:
            # prevents endless recursion, stops after 6 iterations (arbitrary choice)...if no results found after 6, something is wrong
            print('\nError! No csv results found in 6 most recent containers. Terminating search...')
            return None
        else:
            # recursive call for if no .csv is found in results object...increments depth and tries again
            print('\nNo results csv found. Checking next container...')
            result_csvs += get_result_csv(agent_id, depth=depth+1)
    # returns array of URL(s) of CSV(s) w/ scrape results
    return result_csvs

###############################################################################
# Main
###############################################################################
def _main_():
    # Test functions:
    # data = get_all_agents()
    # print("agents info:\n")
    # print(data)
    # data2 = get_agent_info("4170077962142115")
    # print("agent info:\n")
    # print(data2)
    agent_id = get_agent_id_by_keyword("Profile")
    if agent_id:  
        print("agent id:\n")
        print(agent_id)
        agent_name = get_agent_name_by_id(agent_id)
        print("agent name:\n")
        print(agent_name)
    result_csv = get_result_csv(agent_id)
    print("result csv:\n")
    print(result_csv)

if __name__ == "__main__":
    _main_()
