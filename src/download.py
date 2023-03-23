"""
Extract part of the ETL and QA pipeline.

Import as:
import src.download as ssexbido
"""

import logging

from typing import Any
from serpapi import GoogleSearch
import pandas as pd
import common.download as ssandown

_LOG = logging.getLogger(__name__)


# #############################################################################
# OhlcvRestApiDownloader
# #############################################################################


class OhlcvRestApiDownloader(ssandown.DataDownloader):
    """
    Class for downloading google trends data using REST API provided by SerpAPI.
    """

    def __init__(self) -> None:
        """
        Construct google ternds downloader class instance.
        """

    def download(self, topic: str, **kwargs) -> Any:

        # headers
        params = {
            "engine": "google_trends",
            "q": topic,
            "data_type": "TIMESERIES",
            "date": "all",
            "api_key": "a6089e527a003d73adcb30646872ead71c83df40135fb2dd19848fa1c1d12644"
        }

        # performing a query
        search = GoogleSearch(params)
        results = search.get_dict()
        interest_over_time = results["interest_over_time"]

        # formatting the data
        columns = ["Time", "Frequency"]
        cleaned_data = []
        raw_data = interest_over_time["timeline_data"]

        for record in raw_data:
            cleaned_data.append(
                [
                    record["date"].replace("\u2009", " "),
                    record["values"][0]["extracted_value"]
                ]
            )

        # constructing a dataframe out of the data
        df = pd.DataFrame(cleaned_data, columns=columns)
        print(df.head(5))

        return df