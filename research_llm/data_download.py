# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.15.2
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Data Download
#
# We deal with downloading data from the following sources:
# - [goperigon.com](https://goperigon.com)
# - [newsdata.io](https://newsdata.io)
# - [mediastack.com](https://mediastack.com)
# - [thenewsapi.com](https://thenewsapi.com)
# - [marketaux.com](https://marketaux.com)
#

# %%
import requests
import logging
import json

_LOG = logging.getLogger(__name__)


# %% [markdown]
# ## goperigon.com pipeline

# %%
class goperigonPipeline:
    """
    Handles fetching data from the goperigon.com API.
    """
    def __init__(self, api_key: str = 'f745aea8-78bf-4a63-b98d-ba22320b90ad'):
        """
        Initializes the pipeline.

        :param api_key: API key for authentication.
        """
        self.api_key = api_key
        self.base_url = "https://api.goperigon.com/v1/"
        self.headers = {"Authorization": f"Bearer {api_key}"}
        _LOG.debug(f"{self.__class__.__name__} initialized with API key: {api_key}")

    def set_query_params(self, endpoint: str = 'all', **kwargs) -> None:
        """
        Specify the API endpoint and the query parameters for fetching data.

        :param endpoint: The API endpoint to fetch data from.
            - 'all'        : Provides functionality for searching and filtering all 
                             news articles available on the API.
            - 'stories/all': Fetches data from all the sources.
            - 'sources'    : Provides functionality to search and filter for media 
                             sources from around the world.
            - 'journalists': Provides functionality for searching and filtering all journalists 
                             available on the database. 
            - 'people/all' : Search and retrieve additional information on known persons that 
                             exist within Perigon's 
                             entity database and as referenced in any article response object.
            - 'companies'  : Search and retrieve additional information on companies that exist within Perigon's 
                             entity database and as referenced in any article response object.
        :param **kwargs: The query parameters to be passed. E.g., 'category', 'topic', etc. Visit 
                        `https://docs.goperigon.com/docs/getting-started` for more information on query params for each endpoint.
        """
        # Check if kwargs is empty and assign default query params.
        if not kwargs:  
            self.params = {
            "category": 'Finance',
            "topic": 'Cryptocurrency'
            }
        else:
            self.params = kwargs
        # Assign the input endpoint as an instance variable.
        if endpoint in ['all', 'stories/all', 'sources', 'journalists', 'people/all', 'companies']:
            self.endpoint = endpoint
        else:
            raise ValueError(f"Invalid endpoint: {endpoint}")
        
    def fetch_data(self) -> dict:
        """
        Fetches data from a specified endpoint of the goperigon.com API.

        :return: The JSON response from the API.
        """
        # Create the URL to fetch the data specifying the endpoint.
        url = self.base_url + self.endpoint
        # Fetch data from the API endpoint with the passed parameters.
        try:
            response = requests.get(url=url, headers=self.headers, params=self.params)
            _LOG.debug(f"Fetching data from {response.request.url}")
            response.raise_for_status()
            _LOG.info("Data fetched successfully.")
            return response.json()
        except requests.RequestException as e:
            _LOG.error(f"Error fetching data: {e}")
            raise



# %% [markdown]
# ### Sample data
#

# %%
datapipeline_1 = goperigonPipeline()
datapipeline_1.set_query_params()
response_1 = datapipeline_1.fetch_data()
print(json.dumps(response_1, indent = 4))


# %% [markdown]
# ## newsdata.io pipeline

# %%
class newsdataPipeline:
    """
    Handles fetching data from the newsdata.io API. 
    """
    def __init__(self, api_key: str = 'pub_41599cecccb2e7df362a6da41983f3f8729dd'):
        """
        Initializes the pipeline. 

        :param api_key: API key for authentication.
        """
        self.base_url = 'https://newsdata.io/api/1/'
        self.headers = {"X-ACCESS-KEY": api_key}
        _LOG.debug(f"{self.__class__.__name__} initialized with API key: {api_key}")

    def set_query_params(self, endpoint: str = 'news', **kwargs) -> None:
        """
        Specify the query parameters for fetching data. 
        
        :param endpoint: The API endpoint to fetch data from.
            - 'news'    : Provides access to the latest and breaking news upto the past 48 hrs.
            - 'crypto'  : Provided crypto related news and blog data.
            - 'archive' : Provides access to the old news data upto past 2 years.
            - 'sources' : Provides names of randomly selected 100 domains from a country, category or/and language.
        :param **kwargs: The query parameters to be passed. Visit `https://newsdata.io/documentation` 
                         for more information on query params for each endpoint.
        """
        # Check if kwargs is empty and assign default values.
        if not kwargs:
            self.params = {
                'q': "cryptocurrency"
                 }   
        else:
            self.params = kwargs
         # Assign the input endpoint as an instance variable.
        if endpoint in ['news', 'crypto', 'archive', 'sources']:
            self.endpoint = endpoint
        else:
            raise ValueError(f"Invalid endpoint: {endpoint}")    

    def fetch_data(self) -> dict:
        """
        Fetches news data from NewsData.io.

        :return: The JSON response from the API. 
        """
        # Create the URL to fetch the data specifying.
        url = self.base_url + self.endpoint
        # Fetch data from the API with the passed parameters.
        try:
            response = requests.get(url=url, headers=self.headers, params=self.params)
            _LOG.debug(f"Fetching data from {response.request.url}")
            response.raise_for_status()
            _LOG.info("Data fetched successfully.")
            return response.json()
        except requests.RequestException as e:
            _LOG.error(f"Error fetching data: {e}")
            raise



# %% [markdown]
# ### Sample Data

# %%
datapipeline_2 = newsdataPipeline()
datapipeline_2.set_query_params()
response_2 = datapipeline_2.fetch_data()
print(json.dumps(response_2, indent=4))


# %% [markdown]
# ## thenewsapi.com pipeline

# %%
class newsapiPipeline:
    """
    Handles fetching data from the thenewsapi.com API.
    """
    def __init__(self, api_key: str = 'Nr11NvvuUR3VkVqFbWQpDFxdAA9wClTkofsFtQ9l'):
        """
        Intializes the pipeline.

        :param api_key: API key for authentication.
        """
        # Assign the passed API to an instance variable.
        self.api_key = api_key
        self.base_url = 'https://api.thenewsapi.com/v1/news/'
        _LOG.debug(f"{self.__class__.__name__} initialized with API key: {api_key}")

    def set_query_params(self, endpoint: str = 'all', **kwargs) -> None:
        """
        Specify the query parameters for fetching data. 
        
        :param endpoint: The API endpoint to fetch data from.
            - 'headlines' : Get the latest headlines by category.
            - 'top'       : Find live and historical top stories around the world. 
            - 'all'       : Find all live and historical articles.
            - 'similar'   : Find similar stories to a specific article based on its UUID.
            - 'uuid'      : Find specific articles by the UUID.
            - 'sources'   : Find sources to use in your news API requests.
        :param **kwargs: The query parameters to be passed. Visit `https://www.thenewsapi.com/documentation` 
                         for more information on query params for each endpoint.
        """
        # Check if kwargs is empty and assign default values.
        if not kwargs:
            self.params = {
                'api_token': self.api_key,
                'search': "crypto | cryptocurrency"
                 }   
        else:
            self.params = kwargs
            # Pass API key as query paramter.
            self.params['api_token']=self.api_key
         # Assign the input endpoint as an instance variable.
        if endpoint in ['headlines', 'top', 'all', 'similar', 'uuid', 'sources']:
            self.endpoint = endpoint
        else:
            raise ValueError(f"Invalid endpoint: {endpoint}")    

    def fetch_data(self, uuid: str=None) -> dict:
        """
        Fetches top news data from thenewsapi.com.
        
        :param uuid: UUID value if `endpoint = 'similar'` or `endpoint = 'uuid'`.
        :return: The JSON response from the API.
        """
        # Check if endpoint if 'uuid' or 'similar' and create the URL to fetch data.
        if self.endpoint in ['uuid', 'similar']:
            if uuid is not None: 
                url = self.base_url + self.endpoint + '/' + uuid
            else:
                raise ValueError("Enter Valid UUID.")
        else:
            url = self.base_url + self.endpoint        
        # Fetch data from the API with the passed parameters.
        try:
            response = requests.get(url=url, params=self.params)
            _LOG.debug(f"Fetching data from {response.request.url}")
            response.raise_for_status()
            _LOG.info("Data fetched successfully.")
            return response.json()
        except requests.RequestException as e:
            _LOG.error(f"Error fetching data: {e}")
            raise



# %% [markdown]
# ### sample data

# %%
datapipeline_3 = newsapiPipeline()
datapipeline_3.set_query_params()
response_3 = datapipeline_3.fetch_data()
print(json.dumps(response_3, indent=4))


# %% [markdown]
# ## marketaux.com pipeline

# %%
class marketauxPipeline:
    """
    Handles fetching data from the marketaux.com API.
    """
    def __init__(self, api_key: str = 'shRM7CyfkEGsBK83T3IB7YweAiKbzORMZjEWERtu'):
        """
        Intializes the pipeline.

        :param api_key: API key for authentication.
        """
        # Assign the passed API to an instance variable.
        self.api_key = api_key
        self.base_url = 'https://api.marketaux.com/v1/'
        _LOG.debug(f"{self.__class__.__name__} initialized with API key: {api_key}")

    def set_query_params(self, endpoint: str = 'news/all', **kwargs) -> None:
        """
        Specify the query parameters for fetching data. 
        
        :param endpoint: The API endpoint to fetch data from.
            - 'news/all'       : Find all live and historical articles.
            - 'news/similar'   : Find similar stories to a specific article based on its UUID.
            - 'news/uuid'      : Find specific articles by the UUID.
            - 'news/sources'   : Find sources to use in your news API requests.
            - 'entity/stats'   : Get an intraday view of how well entities performed over 
                                 different intervals using this endpoint.
            - 'entity/stats/aggregation'    : Returns an aggregation of entities for a single time frame, 
                                              rather than being broken down by date.
            - 'entity/trending/aggregation' : Use this endpoint to identify trending entities.
            - 'entity/search'               : Use this endpoint to search for all supported entities.
            - 'entity/type/list'            : Use this endpoint to return all supported entity types.
            - 'entity/industry/list'        : Use this endpoint to return all supported entity industries.
        :param **kwargs: The query parameters to be passed. Visit `https://www.marketaux.com/documentation` 
                         for more information on query params for each endpoint.
        """
        # Check if kwargs is empty and assign default values.
        if not kwargs:
            self.params = {
                'api_token': self.api_key,
                'search': "crypto | cryptocurrency"
                 }   
        else:
            self.params = kwargs
            # Pass API key as query paramter.
            self.params['api_token']=self.api_key
         # Assign the input endpoint as an instance variable.
        if endpoint in ['news/all', 'news/similar', 'news/uuid', 'news/sources', 'entity/stats', 
                        'entity/stats/aggregation', 'entity/tending/aggregation', 'entity/search'
                        'entity/type/list', 'entity/industry/list']:
            self.endpoint = endpoint
        else:
            raise ValueError(f"Invalid endpoint: {endpoint}")    

    def fetch_data(self, uuid: str=None) -> dict:
        """
        Fetches top news data from marketaux.com.
        
        :param uuid: UUID value if `endpoint = 'similar'` or `endpoint = 'uuid'`.
        :return: The JSON response from the API.
        """
        # Check if endpoint if 'uuid' or 'similar' and create the URL to fetch data.
        if self.endpoint in ['uuid', 'similar']:
            if uuid is not None: 
                url = self.base_url + self.endpoint + '/' + uuid
            else:
                raise ValueError("UUID is None.")
        else:
            url = self.base_url + self.endpoint        
        # Fetch data from the API with the passed parameters.
        try:
            response = requests.get(url=url, params=self.params)
            _LOG.debug(f"Fetching data from {response.request.url}")
            response.raise_for_status()
            _LOG.info("Data fetched successfully.")
            return response.json()
        except requests.RequestException as e:
            _LOG.error(f"Error fetching data: {e}")
            raise



# %% [markdown]
# ### sample data

# %%
datapipeline_4 = marketauxPipeline()
datapipeline_4.set_query_params()
response_4 = datapipeline_4.fetch_data()
print(json.dumps(response_4, indent=4))

# %%
