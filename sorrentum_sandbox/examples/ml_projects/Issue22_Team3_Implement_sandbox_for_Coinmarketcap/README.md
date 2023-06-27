# Issue22 Team3 Implement sandbox for CoinMarketCap

Implement the download_to_jsonfile and download_to_db for api v1/cryptocurrency/listings/latest.

## 0. Content

**Main python files:** 

- `db.py`: contains the interface to load / save CoinMarketCap raw data to MongoDB
  	i.e., "Load stage" of an ETL pipeline
- `download.py`: implement the logic to download the data from CoinMarketCap
  	i.e., "Extract stage"
- `download_to_jsonfile.py`: implement Extract stage to local json file
- `download_to_db.py`: implement Extract stage to MongoDB

**One Jupyternote book:**

- `DATA605_DATA_Download.ipynb`: download through api v1/cryptocurrency/listings/latest

**Output Files:**

- `CoinMarketCap.json` : the output of download_to_jsonfile.py, it downloads date from CoinMarketCap and save it in this file
- `The Cvs file under coinMarketCap_data`: the output of  tmp/download_to_csv.py

**Other Files:**

- `/tmp/download_cmc.py`  and `/tmp/download_to_csv_cmc.py` : download data and save to css file, but because I decided to use to mongodb, so these files are not main files now

- `api_cmc.py` : try to implement a file for multiple apis, haven't finished it yet.

- And some log files.

  

## 1. Setup

- Need to set CoinMarketCap API key to use the code in this directory

- See the guide to apply a API key for CoinMarketCap from here :

   https://coinmarketcap.com/api/documentation/v1/#section/Quick-Start-Guide

- The set the `.env` file under `sorrentum_sandbox/devops`

```python
# CoinMarketCap API Key
COINMARKETCAP_API_KEY=your api key
```



## 2. download_to_jsonfile.py

- Run in docker, usage:

  ```
  docker> ./download_to_jsonfile.py --start 1 --limit 200
  ```

   `Start` : integer >= 1

  Optionally offset the start (1-based index) of the paginated list of items to return.

  

   `limit`: integer [ 1 .. 5000 ]

  Optionally specify the number of results to return. Use this parameter and the "start" parameter to determine your own pagination size.

## 3. download_to_db.py

- Run in docker, usage:

  ```
  docker> ./download_to_db.py --start 1 --limit 200 --collection_name cmc_date
  ```

- start and limit: same with download_to_jsonfile
- collection_name: collection_name for mongodb to save data.