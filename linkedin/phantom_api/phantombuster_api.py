from pydantic import BaseModel
from tqdm import tqdm
import pandas as pd
import pyinputplus as pyip
import datetime
import json
import requests
import utils
import config
import config as p_config
from typing import List, Optional

# Getting API Key from config.py
API_KEY = p_config.PHANTOMBUSTER_API

###############################################################################
# Phantom API
###############################################################################
def _get_phantom_data(api_key: str, url: Optional[str] = "https://api.phantombuster.com/api/v1/user") -> dict:
    """
    Get phantom information from Phantombuster.
    
    Parameters:
    api_key: The Phantombuster API key.
    url: The URL. Default is "https://api.phantombuster.com/api/v1/user".
    
    Returns:
    A dictionary containing the phantoms information, or None if the request failed.
    """
    headers = {"accept": "application/json", "X-Phantombuster-Key": api_key}
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
        return None
    return response.json()


def get_all_phantoms(api_key: str) -> Optional[pd.DataFrame]:
    """
    Get all phantoms information from Phantombuster.
    URL: https://api.phantombuster.com/api/v1/user

    Returns:
    A dataframe containing the phantoms information, or None if the request failed.
    """
    data = _get_phantom_data(api_key)
    if data is not None and "data" in data and "agents" in data["data"]:
        return pd.DataFrame(data["data"]["agents"])
    else:
        print("Error: Invalid data structure.")
        return None


def get_phantom_id_by_keyword(data: pd.DataFrame, keyword: Optional[str] = None) -> Optional[int]:
    """
    Get agent id by keyword, the most recent one Found.
    If no keyword, return the most recent agent.
    Ignore case.
    """
    if data is None or data.empty:
        print("No data available.")
        return None
    #
    if keyword:
        data = data[data["name"].str.lower().str.contains(keyword.lower())]
        if data.empty:
            print(f"No agent found with keyword [ {keyword} ]")
            return None
    #
    return data.iloc[-1]["id"]


def get_phantom_info(phantom_id: str, api_key: str) -> Optional[dict]:
    """
    Get agent info from Phantombuster.
    """
    url = f"https://api.phantombuster.com/api/v2/agents/fetch?id={phantom_id}"
    headers = {"accept": "application/json", "X-Phantombuster-Key": api_key}
    request = requests.get(url, headers=headers, timeout=30)
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
        return None
    return response.json()


def get_phantom_name_by_id(phantom_id: str, api_key: str) -> Optional[str]:
    """
    Get agent name by id.
    """
    data = get_phantom_info(phantom_id, api_key)
    if data is not None and "name" in data:
        return data["name"]
    else:
        print("Error: Invalid data structure.")
        return None


###############################################################################
# Container API
###############################################################################
def get_containers_id_by_phantom_id(phantom_id: str, api_key: str) -> Optional[List[str]]:
    """
    Get all container IDs by phantom ID.

    Parameters:
    api_key: The Phantombuster API key.
    phantom_id: The ID of the phantom for which to fetch the container IDs.

    Returns:
    A list of container IDs for the given phantom, or None if the request failed.
    """
    url = f"https://api.phantombuster.com/api/v2/containers/fetch-all?agentId={phantom_id}"
    headers = {"accept": "application/json", "X-Phantombuster-Key": api_key}
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
        return None
    # 
    data = response.json().get("containers", [])
    containers_id = [container["id"] for container in data]
    # 
    return containers_id


def get_result_csv_by_container_id(container_id: str, api_key: str) -> Optional[str]:
    """
    Get result CSV by container ID.

    Parameters:
    api_key: The Phantombuster API key.
    container_id: The ID of the container for which to fetch the result CSV.

    Returns:
    A string containing the result CSV, or None if the request failed.
    """
    url = f"https://api.phantombuster.com/api/v2/containers/fetch-result-object?id={container_id}"
    headers = {"accept": "application/json", "X-Phantombuster-Key": api_key}
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
        return None
    # 
    return response.text


def get_container_info(container_id: str, api_key: str) -> Optional[dict]:
    """
    Get container info from Phantombuster.
    """
    url = f"https://api.phantombuster.com/api/v2/containers/fetch?id={container_id}"
    headers = {"accept": "application/json", "X-Phantombuster-Key": api_key}
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
        return None
    # 
    return response.json()


def get_last_updated_time(container_id: str, api_key: str) -> Optional[datetime.datetime]:
    """
    Get the last updated time for a given container ID.

    Parameters:
    container_id: The ID of the container.
    api_key: The Phantombuster API key.

    Returns:
    A datetime object representing the last updated time, or None if the request failed or the container info was invalid.
    """
    container_info = get_container_info(container_id, api_key)
    if container_info is None or "endedAt" not in container_info:
        print(f"Error: Failed to get info for container ID {container_id}.")
        return None

    # The timestamp is in milliseconds, convert it to seconds
    timestamp_seconds = container_info["endedAt"] / 1000
    last_updated_time = datetime.datetime.fromtimestamp(timestamp_seconds)
    # 
    return last_updated_time


# TODO: (Yiyun) Update to use API containers/fetch-output.
def get_result_csv_by_phantom_id(phantom_id: str, api_key: str, depth: int = 10) -> Optional[List[str]]:
    """
    Get the most recent result CSVs by phantom ID.

    Parameters:
    api_key: The Phantombuster API key.
    phantom_id: The ID of the phantom for which to fetch the result CSV.
    depth: The depth of the search. If None, process all container IDs.

    Returns:
    A list of result CSV URLs, or None if the request failed.
    """
    containers_id = get_containers_id_by_phantom_id(phantom_id, api_key)
    if containers_id is None:
        print("Error: Failed to get container IDs.")
        return None
    
    # If depth is None, process all container IDs. Otherwise, process the specified number of container IDs.
    containers_to_process = containers_id if depth is None else containers_id[:depth]
    # 
    print("Begin to fetch result CSV ...")
    for container_id in tqdm(containers_to_process):
        response = get_result_csv_by_container_id(container_id, api_key)
        if response is None:
            print(f"Error: Failed to get result CSV for container ID {container_id}.")
            continue
        if 'csv' in response:
            response_array = response.split('"')
            csv_url = next((i.replace('\\', '') for i in response_array if ".csv" in i), None)
            if csv_url:
                print(f"\nFetch result csv successfully for {container_id}!")
                last_updated_time = get_last_updated_time(container_id, api_key)
                print(f"Last updated time for result CSV is: {last_updated_time}")
                return csv_url
    # 
    return None


# TODO:(Yiyun) deprecate this function.
def get_search_result_by_phantom_id(phantom_id: str, api_key: str, depth: int = 10) -> Optional[pd.DataFrame]:
    containers_id = get_containers_id_by_phantom_id(phantom_id, api_key)
    if containers_id is None:
        print("Error: Failed to get container IDs.")
        return None
    # If depth is None, process all container IDs. Otherwise, process the specified number of container IDs.
    containers_to_process = containers_id if depth is None else containers_id[:depth]
    # 
    print("begin to fetch search result ...")
    for container_id in tqdm(containers_to_process):
        response = get_result_csv_by_container_id(container_id, api_key)
        if response is None:
            print(f"Error: Failed to get result CSV for container ID {container_id}.")
            continue
        if 'profileUrl' in response:
            data_list = json.loads(data)
            df = pd.DataFrame(data_list)
            print(f"\nFetch search result successfully for {container_id}!")
            last_updated_time = get_last_updated_time(container_id, api_key)
            print(f"Last updated time for search result is: {last_updated_time}")
            return df
        else:
            print(f"Error: Failed to get search result for container ID {container_id}.")
            print(response)
    return None
    

###############################################################################
# Main
###############################################################################
def _main_():
    print(get_all_phantoms(API_KEY))
    # print(get_phantom_id_by_keyword(get_all_phantoms(API_KEY), "linkedin"))
    phantom_id = get_phantom_id_by_keyword(get_all_phantoms(API_KEY), "GP LinkedIn Profile Scraper")
    # print(get_phantom_info(phantom_id, API_KEY))
    print(get_phantom_name_by_id(phantom_id, API_KEY))
    result_csvs = get_result_csv_by_phantom_id(phantom_id, API_KEY, depth=None)
    result = get_search_result_by_phantom_id(phantom_id, API_KEY, depth=None)
    print(result)
    

if __name__ == "__main__":
    _main_()
