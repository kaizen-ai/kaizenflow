"""
Import as:

import linkedin.phantom_api.phantombuster_api as lpphapia
"""

import json
import logging
import re
from typing import Any, Dict, List, Optional

import pandas as pd
import requests

import helpers.hio as hio
import linkedin.config.phantom_api_key as lcphapke

_LOG = logging.getLogger(__name__)


class Phantom:
    def __init__(self) -> None:
        self.api_key = lcphapke.PHANTOMBUSTER_API

    def get_all_phantoms(self) -> Optional[pd.DataFrame]:
        """
        Retrieve all the names and IDs of the Phantoms from Phantombuster.

        :return: a dataframe containing the all Phantoms information,
            or None if the request failed
        Example of the return dataframe:
        # pylint: disable=line-too-long
        ```
                      id            name scriptId ... lastEndStatus queuedContainers runningContainers
        2862499141527492   Search Export     6988           success                0                 0
        3593602419926765 Profile Scraper     3112           success                0                 0
        3933308360008191 Profile Scraper     3112           success                0                 0
        # pylint: enable=line-too-long
        ```
        """
        data = self._get_phantom_data()

        if data and "data" in data and "agents" in data["data"]:
            return pd.DataFrame(data["data"]["agents"])
        #
        _LOG.error("The data structure is invalid: Phantoms not found.")
        return None

    def download_result_csv_by_phantom_id(
        self, phantom_id: str, output_path: str
    ) -> None:
        """
        Download result CSV by phantom ID.

        :param phantom_id: The ID of the phantom for which to fetch the result CSV
        :param output_path: The path to save the result CSV
        """
        result_url = self._get_result_csv_by_phantom_id(phantom_id)
        if result_url:
            self._download_result_csv_to_local(result_url, output_path)

    # #########################################################################

    @staticmethod
    def _download_result_csv_to_local(
        result_csv_url: str, output_path: str
    ) -> None:
        """
        Download result CSV from Phantombuster server to local storage.

        :param result_csv_url: The URL of the result CSV
        :param output_path: The path to save the result CSV
        """
        hio.create_enclosing_dir(output_path, incremental=True)
        try:
            response = requests.get(result_csv_url, timeout=30)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            _LOG.error("%s", e)
            return None
        hio.to_file(output_path, response.text)
        _LOG.info("Result CSV saved to %s", output_path)
        return None

    @staticmethod
    def _extract_result_csv_url(response_text: str) -> Optional[str]:
        """
        Extract the result CSV URL from the response text.

        :param response_text: The response text from Phantom API
            containers/fetch-output
        :return: a string containing the result CSV url,
            or None if there is no result CSV url
        """
        data = json.loads(response_text)
        output = data["output"]
        csv_url = re.search("CSV saved at (https://[^\\s]+)", output)
        if csv_url:
            csv_url = csv_url.group(1)
        else:
            _LOG.warning("Can't find the result CSV url.")
            return None
        return csv_url

    # #########################################################################
    # Private functions.
    # #########################################################################
    def _get_result_csv_by_phantom_id(self, phantom_id: str) -> Optional[str]:
        """
        Get result CSV by Phantom id.

        :param phantom_id: The id of the phantom for which to
            fetch the result CSV
        :return: a string containing the result CSV url,
            or None if there is no result CSV url
        """
        containers_id = self._get_all_containers_id_by_phantom_id(phantom_id)
        if containers_id is None:
            _LOG.error(
                "There is no container id available. Have you run the Phantom?"
            )
            return None
        # Get the last container ID, which is from the first time Phantom ran.
        # Later runs might not provide the CSV link if there's no new data.
        # We use the first run to ensure we obtain the CSV link, which remains
        # consistent across all containers of a Phantom.
        container_to_process = containers_id[-1]
        #
        result_csv_url = self._get_result_csv_by_container_id(
            container_to_process
        )
        if result_csv_url is None:
            _LOG.error("Failed to fetch result CSV.")
            return None
        #
        return result_csv_url

    def _get_all_containers_id_by_phantom_id(
        self, phantom_id: str
    ) -> Optional[List[str]]:
        """
        Get all container IDs by the Phantom ID.

        Containers represent the executions of a Phantom.
        Each time a Phantom runs, a new container is created.
        Thus, if you run a Phantom 10 times, you will have 10 containers.
        Every container has a unique container ID.

        :param phantom_id: The ID of the phantom for which to get containers
        :return: a list containing all containers id,
            or None if the request failed
        """
        url = f"https://api.phantombuster.com/api/v2/containers/fetch-all?agentId={phantom_id}"
        response = self._get_api_response(url)
        if response is None:
            return None
        #
        data = response.json().get("containers", [])
        containers_id = [container["id"] for container in data]
        #
        return containers_id

    def _get_result_csv_by_container_id(self, container_id: str) -> Optional[str]:
        """
        Get result CSV url by container id.

        :param container_id: The id of the container for which to fetch the result CSV
        :return: a string containing the result CSV url, or None if there is no result CSV url
        """
        url = f"https://api.phantombuster.com/api/v2/containers/fetch-output?id={container_id}"
        response = self._get_api_response(url)
        if response is None:
            return None
        # Extract the result CSV URL from the response text.
        result_csv_url = self._extract_result_csv_url(response.text)
        if result_csv_url:
            _LOG.info("Result CSV URL: %s", result_csv_url)
            return result_csv_url
        return None

    def _get_phantom_data(self) -> Optional[Dict[str, Any]]:
        """
        Get all Phantoms information from Phantombuster.

        :return: a dictionary containing all Phantoms information

        Example of the returned data:
        ```
        {'status': 'success',
         'data': {'email': 'saggese@gmail.com',
          'timeLeft': 72000,
          'emailsLeft': 100,
          'storageLeft': 993000000,
          'captchasLeft': 1000,
          'databaseLeft': 0,
          'agents': [{'id': 3593602419926765,
            'name': 'Yiyun LinkedIn Profile Scraper',
            'scriptId': 3112,
            'lastEndMessage': '',
            'lastEndStatus': 'success',
            'queuedContainers': 0,
            'runningContainers': 0},
           {'id': 4809903115550081,
            'name': 'Retired MBA DC UMD LinkedIn Search Export',
            'scriptId': 3149,
            'lastEndMessage': '',
            'lastEndStatus': 'success',
            'queuedContainers': 0,
            'runningContainers': 0}]}}
        ```
        """
        url = "https://api.phantombuster.com/api/v1/user"
        response = self._get_api_response(url)
        if response is None:
            return None
        return response.json()

    def _get_api_response(self, url: str) -> Optional[requests.Response]:
        """
        Get API response data.

        :param url: The url of the API request
        :return: a dictionary containing the API response data,
            or None if the request failed
        """
        headers = {
            "accept": "application/json",
            "X-Phantombuster-Key": self.api_key,
        }
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            _LOG.error("%s", e)
            return None
        return response
