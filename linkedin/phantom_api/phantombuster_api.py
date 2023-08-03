from tqdm import tqdm
import pandas as pd
import datetime
import json
import re
import os
import requests
import linkedin.config.phantom_api_key as config
from typing import List, Optional

class Phantom:
    def __init__(self):
        # Getting API Key from config.py
        self.api_key =  config.PHANTOMBUSTER_API

    def get_all_phantoms(self) -> Optional[pd.DataFrame]:
        """
        Get all phantoms information from Phantombuster.
        URL: https://api.phantombuster.com/api/v1/user

        Returns:
        A dataframe containing the phantoms information, or None if the request failed.
        """
        data = self._get_phantom_data()
        if data is not None and "data" in data and "agents" in data["data"]:
            return pd.DataFrame(data["data"]["agents"])
        else:
            print("Error: Invalid data structure.")
            return None

    def download_result_csv_by_phantom_id(self, phantom_id: str, output_path: str) -> None:
        reuslt_url = self._get_result_csv_by_phantom_id(phantom_id)
        if reuslt_url:
            self._download_result_csv_to_lcoal(reuslt_url, output_path)
        return None
        
    ###########################################################################
    def _get_phantom_data(self, url: Optional[str] = "https://api.phantombuster.com/api/v1/user") -> dict:
        """
        Get phantom information from Phantombuster.
        Params:
        url: The URL. Default is "https://api.phantombuster.com/api/v1/user".
        Returns:
        A dictionary containing the phantoms information, or None if the request failed.
        """
        headers = {"accept": "application/json", "X-Phantombuster-Key": self.api_key}
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error: {e}")
            return None
        return response.json()

    def _download_result_csv_to_lcoal(self, result_csv_url: str, output_path: str) -> None:
        """
        Download result CSV to local.
        """
        # Check if directory exists, create if not
        directory = os.path.dirname(output_path)
        if not os.path.exists(directory):
            os.makedirs(directory)

        try:
            response = requests.get(result_csv_url, timeout=30)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error: {e}")
            return None
        # 
        with open(output_path, "wb") as f:
            f.write(response.content)
        # 
        print(f"File downloaded successfully to {output_path}")
        return None

    def _get_containers_id_by_phantom_id(self, phantom_id: str) -> Optional[List[str]]:
        """
        Get all container IDs by phantom ID.
        Params:
        phantom_id: The ID of the phantom for which to fetch the container IDs.
        Returns:
        A list of container IDs for the given phantom, or None if the request failed.
        """
        url = f"https://api.phantombuster.com/api/v2/containers/fetch-all?agentId={phantom_id}"
        headers = {"accept": "application/json", "X-Phantombuster-Key": self.api_key}
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

    def _extract_result_csv_url(self, response_text: str) -> Optional[str]:
        data = json.loads(response_text)
        output = data['output']
        csv_url = re.search('CSV saved at (https://[^\\s]+)', output)
        if csv_url:
            csv_url = csv_url.group(1)
        else:
            print("Can't find the result CSV URL.")
            return None
        return csv_url

    def _get_result_csv_by_container_id(self, container_id: str) -> Optional[str]:
        """
        Get result CSV by container ID.
        Params:
        api_key: The Phantombuster API key.
        container_id: The ID of the container for which to fetch the result CSV.
        Returns:
        A string containing the result CSV, or None if the request failed.
        """
        url = f"https://api.phantombuster.com/api/v2/containers/fetch-output?id={container_id}"
        headers = {"accept": "application/json", "X-Phantombuster-Key": self.api_key}
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error: {e}")
            return None
        # Extract the result CSV URL from the response text.
        result_csv_url = self._extract_result_csv_url(response.text)
        if result_csv_url:
            print(f"Result CSV URL: {result_csv_url}")
            return result_csv_url
        else:
            return None

    def _get_result_csv_by_phantom_id(self, phantom_id: str) -> Optional[str]:
        """
        Get the most result csv by phantom ID.
        Parameters:
        phantom_id: The ID of the phantom for which to fetch the result CSV.
        """
        containers_id = self._get_containers_id_by_phantom_id(phantom_id)
        if containers_id is None:
            print("There is no container id available.")
            return None
        # Get the oldest/first container id, as the result url won't change and first container always get result url.
        container_to_process = containers_id[-1]
        # 
        result_csv_url = self._get_result_csv_by_container_id(container_to_process)
        if result_csv_url is None:
            print(f"Error: Failed to fetch result CSV.")
            return None
        # 
        return result_csv_url
