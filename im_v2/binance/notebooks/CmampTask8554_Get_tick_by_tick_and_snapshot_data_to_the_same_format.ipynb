{
 "cells": [
  {
   "cell_type": "markdown",
   "id": "1cb361ca",
   "metadata": {},
   "source": [
    "# Get Tick-by-tick and snapshot data to the same format "
   ]
  },
  {
   "cell_type": "markdown",
   "id": "13651e2f",
   "metadata": {},
   "source": [
    "## Imports"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 92,
   "id": "ab6b242a",
   "metadata": {
    "ExecuteTime": {
     "end_time": "2024-06-20T19:52:48.305002Z",
     "start_time": "2024-06-20T19:52:48.301447Z"
    }
   },
   "outputs": [],
   "source": [
    "import datetime\n",
    "import logging\n",
    "\n",
    "import pandas as pd\n",
    "\n",
    "import helpers.hdbg as hdbg\n",
    "import helpers.henv as henv\n",
    "import helpers.hpandas as hpandas\n",
    "import helpers.hprint as hprint\n",
    "import im_v2.common.data.client.im_raw_data_client as imvcdcimrdc\n",
    "import copy"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 93,
   "id": "2509b74a",
   "metadata": {
    "ExecuteTime": {
     "end_time": "2024-06-20T19:52:51.950683Z",
     "start_time": "2024-06-20T19:52:48.642689Z"
    }
   },
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Requirement already satisfied: sortedcontainers in /venv/lib/python3.9/site-packages (2.4.0)\r\n"
     ]
    }
   ],
   "source": [
    "! sudo /venv/bin/pip install sortedcontainers"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 94,
   "id": "bd3379e9",
   "metadata": {
    "ExecuteTime": {
     "end_time": "2024-06-20T19:52:51.957682Z",
     "start_time": "2024-06-20T19:52:51.954381Z"
    }
   },
   "outputs": [],
   "source": [
    "from sortedcontainers import SortedDict"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 95,
   "id": "304a1230",
   "metadata": {
    "ExecuteTime": {
     "end_time": "2024-06-20T19:52:52.569853Z",
     "start_time": "2024-06-20T19:52:51.960397Z"
    }
   },
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "\u001b[0m\u001b[33mWARNING\u001b[0m: Logger already initialized: skipping\n",
      "INFO  # Git\n",
      "  branch_name='CmampTask8666_Collect_1_day_of_orderbook_data_using_our_script_maintain_local_orderbook_copy.py'\n",
      "  hash='4a5277f28'\n",
      "  # Last commits:\n",
      "    * 4a5277f28 jsmerix  Bug fix                                                           (   3 hours ago) Thu Jun 20 16:45:45 2024  (HEAD -> CmampTask8666_Collect_1_day_of_orderbook_data_using_our_script_maintain_local_orderbook_copy.py, origin/CmampTask8666_Collect_1_day_of_orderbook_data_using_our_script_maintain_local_orderbook_copy.py)\n",
      "    * d92508cbf jsmerix  Add functionality to store snapshots to a file                    (  30 hours ago) Wed Jun 19 14:05:47 2024           \n",
      "    * 68fead01d Neha Pathak CmTask8626 Smoke tests for get_ccxt_total_balance.py script. (#8633) (  31 hours ago) Wed Jun 19 12:50:36 2024  (origin/master, origin/HEAD, master)\n",
      "# Machine info\n",
      "  system=Linux\n",
      "  node name=3bfe3a23d5d2\n",
      "  release=5.15.0-1058-aws\n",
      "  version=#64~20.04.1-Ubuntu SMP Tue Apr 9 11:12:27 UTC 2024\n",
      "  machine=x86_64\n",
      "  processor=x86_64\n",
      "  cpu count=8\n",
      "  cpu freq=scpufreq(current=2499.998, min=0.0, max=0.0)\n",
      "  memory=svmem(total=33280274432, available=18648256512, percent=44.0, used=14150426624, free=3182755840, active=3635564544, inactive=21383020544, buffers=491487232, cached=15455604736, shared=1204224, slab=4576382976)\n",
      "  disk usage=sdiskusage(total=218506772480, used=111446290432, free=107043704832, percent=51.0)\n",
      "# Packages\n",
      "  python: 3.9.5\n",
      "  cvxopt: 1.3.2\n",
      "  cvxpy: 1.4.2\n",
      "  gluonnlp: ?\n",
      "  gluonts: ?\n",
      "  joblib: 1.3.2\n",
      "  mxnet: ?\n",
      "  numpy: 1.26.0\n",
      "  pandas: 2.1.1\n",
      "  pyarrow: 15.0.0\n",
      "  scipy: 1.11.3\n",
      "  seaborn: 0.13.0\n",
      "  sklearn: 1.3.1\n",
      "  statsmodels: 0.14.0\n"
     ]
    }
   ],
   "source": [
    "hdbg.init_logger(verbosity=logging.INFO)\n",
    "log_level = logging.INFO\n",
    "\n",
    "_LOG = logging.getLogger(__name__)\n",
    "\n",
    "_LOG.info(\"%s\", henv.get_system_signature()[0])\n",
    "\n",
    "hprint.config_notebook()"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "14162287",
   "metadata": {},
   "source": [
    "## Load Binance Native historical bid/ask data\n",
    "\n",
    "Refer to `im_v2/binance/notebooks/CmampTask8259_Analysis_of_historical_bidask_data.ipynb` for more details"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 96,
   "id": "d2167577",
   "metadata": {
    "ExecuteTime": {
     "end_time": "2024-06-20T19:52:52.577163Z",
     "start_time": "2024-06-20T19:52:52.574390Z"
    }
   },
   "outputs": [],
   "source": [
    "symbol = \"BTCUSDT\""
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 97,
   "id": "df4a0f32",
   "metadata": {
    "ExecuteTime": {
     "end_time": "2024-06-20T19:52:53.452925Z",
     "start_time": "2024-06-20T19:52:52.579743Z"
    },
    "lines_to_next_cell": 2
   },
   "outputs": [],
   "source": [
    "snap_csv = pd.read_csv(f\"/app/{symbol}_T_DEPTH_2024-05-06_depth_snap.csv\")\n",
    "update_csv = pd.read_csv(f\"/app/{symbol}_T_DEPTH_2024-05-06_depth_update.csv\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 98,
   "id": "eee4130d",
   "metadata": {
    "ExecuteTime": {
     "end_time": "2024-06-20T19:52:53.459749Z",
     "start_time": "2024-06-20T19:52:53.455257Z"
    }
   },
   "outputs": [
    {
     "data": {
      "text/plain": [
       "(105738, 9)"
      ]
     },
     "execution_count": 98,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "snap_csv.shape"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 99,
   "id": "6ca8a606",
   "metadata": {
    "ExecuteTime": {
     "end_time": "2024-06-20T19:52:53.472789Z",
     "start_time": "2024-06-20T19:52:53.461701Z"
    }
   },
   "outputs": [
    {
     "data": {
      "text/html": [
       "<div>\n",
       "<style scoped>\n",
       "    .dataframe tbody tr th:only-of-type {\n",
       "        vertical-align: middle;\n",
       "    }\n",
       "\n",
       "    .dataframe tbody tr th {\n",
       "        vertical-align: top;\n",
       "    }\n",
       "\n",
       "    .dataframe thead th {\n",
       "        text-align: right;\n",
       "    }\n",
       "</style>\n",
       "<table border=\"1\" class=\"dataframe\">\n",
       "  <thead>\n",
       "    <tr style=\"text-align: right;\">\n",
       "      <th></th>\n",
       "      <th>symbol</th>\n",
       "      <th>timestamp</th>\n",
       "      <th>trans_id</th>\n",
       "      <th>first_update_id</th>\n",
       "      <th>last_update_id</th>\n",
       "      <th>side</th>\n",
       "      <th>update_type</th>\n",
       "      <th>price</th>\n",
       "      <th>qty</th>\n",
       "    </tr>\n",
       "  </thead>\n",
       "  <tbody>\n",
       "    <tr>\n",
       "      <th>0</th>\n",
       "      <td>BTCUSDT</td>\n",
       "      <td>1714953565827</td>\n",
       "      <td>1714643037593395250</td>\n",
       "      <td>4556869216769</td>\n",
       "      <td>4556869216769</td>\n",
       "      <td>a</td>\n",
       "      <td>snap</td>\n",
       "      <td>66583.4</td>\n",
       "      <td>0.002</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>1</th>\n",
       "      <td>BTCUSDT</td>\n",
       "      <td>1714953565827</td>\n",
       "      <td>1714643037593395250</td>\n",
       "      <td>4556869216769</td>\n",
       "      <td>4556869216769</td>\n",
       "      <td>a</td>\n",
       "      <td>snap</td>\n",
       "      <td>66592.3</td>\n",
       "      <td>0.002</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>2</th>\n",
       "      <td>BTCUSDT</td>\n",
       "      <td>1714953565827</td>\n",
       "      <td>1714643037593395250</td>\n",
       "      <td>4556869216769</td>\n",
       "      <td>4556869216769</td>\n",
       "      <td>a</td>\n",
       "      <td>snap</td>\n",
       "      <td>66729.4</td>\n",
       "      <td>0.002</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>3</th>\n",
       "      <td>BTCUSDT</td>\n",
       "      <td>1714953565827</td>\n",
       "      <td>1714643037593395250</td>\n",
       "      <td>4556869216769</td>\n",
       "      <td>4556869216769</td>\n",
       "      <td>a</td>\n",
       "      <td>snap</td>\n",
       "      <td>66791.4</td>\n",
       "      <td>0.002</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>4</th>\n",
       "      <td>BTCUSDT</td>\n",
       "      <td>1714953565827</td>\n",
       "      <td>1714643037593395250</td>\n",
       "      <td>4556869216769</td>\n",
       "      <td>4556869216769</td>\n",
       "      <td>a</td>\n",
       "      <td>snap</td>\n",
       "      <td>66869.2</td>\n",
       "      <td>0.002</td>\n",
       "    </tr>\n",
       "  </tbody>\n",
       "</table>\n",
       "</div>"
      ],
      "text/plain": [
       "    symbol      timestamp             trans_id  first_update_id  last_update_id side update_type    price    qty\n",
       "0  BTCUSDT  1714953565827  1714643037593395250    4556869216769   4556869216769    a        snap  66583.4  0.002\n",
       "1  BTCUSDT  1714953565827  1714643037593395250    4556869216769   4556869216769    a        snap  66592.3  0.002\n",
       "2  BTCUSDT  1714953565827  1714643037593395250    4556869216769   4556869216769    a        snap  66729.4  0.002\n",
       "3  BTCUSDT  1714953565827  1714643037593395250    4556869216769   4556869216769    a        snap  66791.4  0.002\n",
       "4  BTCUSDT  1714953565827  1714643037593395250    4556869216769   4556869216769    a        snap  66869.2  0.002"
      ]
     },
     "execution_count": 99,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "snap_csv.head()"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "df8fcf23",
   "metadata": {},
   "source": [
    "Confirm the snapshot contains unique timestamp"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 100,
   "id": "6c0f8eb8",
   "metadata": {
    "ExecuteTime": {
     "end_time": "2024-06-20T19:52:53.480203Z",
     "start_time": "2024-06-20T19:52:53.475354Z"
    }
   },
   "outputs": [
    {
     "data": {
      "text/plain": [
       "1"
      ]
     },
     "execution_count": 100,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "snap_csv.timestamp.nunique()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 101,
   "id": "bf7f5c0e",
   "metadata": {
    "ExecuteTime": {
     "end_time": "2024-06-20T19:52:53.487307Z",
     "start_time": "2024-06-20T19:52:53.482222Z"
    }
   },
   "outputs": [
    {
     "data": {
      "text/plain": [
       "Timestamp('2024-05-05 23:59:25.827000')"
      ]
     },
     "execution_count": 101,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "start_timestamp = pd.to_datetime(snap_csv.timestamp.min(), unit=\"ms\")\n",
    "start_timestamp"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "c10aa67c",
   "metadata": {},
   "source": [
    "### Reconstruct order book from snapshot DataFrame"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 141,
   "id": "3e76b563",
   "metadata": {
    "ExecuteTime": {
     "end_time": "2024-06-20T20:03:31.333102Z",
     "start_time": "2024-06-20T20:03:30.968739Z"
    }
   },
   "outputs": [],
   "source": [
    "bids = sorted(snap_csv[snap_csv[\"side\"] == \"b\"][[\"price\", \"qty\"]].values.tolist(), reverse=True)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 142,
   "id": "d9987d86",
   "metadata": {
    "ExecuteTime": {
     "end_time": "2024-06-20T20:03:31.416522Z",
     "start_time": "2024-06-20T20:03:31.362846Z"
    }
   },
   "outputs": [],
   "source": [
    "asks = sorted(snap_csv[snap_csv[\"side\"] == \"a\"][[\"price\", \"qty\"]].values.tolist())"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 144,
   "id": "cbb6ebd1",
   "metadata": {
    "ExecuteTime": {
     "end_time": "2024-06-20T20:04:11.687471Z",
     "start_time": "2024-06-20T20:04:11.673995Z"
    }
   },
   "outputs": [],
   "source": [
    "_ORDER_BOOK_SNAPSHOT = { \n",
    "    \"timestamp\": 1714953565827,\n",
    "    # We only keep top 250 levels\n",
    "    # Bids will need to be accessed in reverse\n",
    "    \"b\": SortedDict(lambda x: -x, {float(price): qty for price, qty in bids[:250]}),\n",
    "    \"a\": SortedDict({price: qty for price, qty in asks[:250]})\n",
    "}"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 106,
   "id": "6c56a1c2",
   "metadata": {
    "ExecuteTime": {
     "end_time": "2024-06-20T19:52:59.140705Z",
     "start_time": "2024-06-20T19:52:59.135428Z"
    }
   },
   "outputs": [
    {
     "data": {
      "text/plain": [
       "Timestamp('2024-05-05 23:59:25.827000')"
      ]
     },
     "execution_count": 106,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "pd.to_datetime(snap_csv.timestamp.max(), unit=\"ms\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 107,
   "id": "4d84c270",
   "metadata": {
    "ExecuteTime": {
     "end_time": "2024-06-20T19:52:59.338763Z",
     "start_time": "2024-06-20T19:52:59.333426Z"
    }
   },
   "outputs": [
    {
     "data": {
      "text/plain": [
       "(712296, 9)"
      ]
     },
     "execution_count": 107,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "update_csv.shape"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 108,
   "id": "989b148e",
   "metadata": {
    "ExecuteTime": {
     "end_time": "2024-06-20T19:52:59.519352Z",
     "start_time": "2024-06-20T19:52:59.508081Z"
    }
   },
   "outputs": [
    {
     "data": {
      "text/html": [
       "<div>\n",
       "<style scoped>\n",
       "    .dataframe tbody tr th:only-of-type {\n",
       "        vertical-align: middle;\n",
       "    }\n",
       "\n",
       "    .dataframe tbody tr th {\n",
       "        vertical-align: top;\n",
       "    }\n",
       "\n",
       "    .dataframe thead th {\n",
       "        text-align: right;\n",
       "    }\n",
       "</style>\n",
       "<table border=\"1\" class=\"dataframe\">\n",
       "  <thead>\n",
       "    <tr style=\"text-align: right;\">\n",
       "      <th></th>\n",
       "      <th>symbol</th>\n",
       "      <th>timestamp</th>\n",
       "      <th>trans_id</th>\n",
       "      <th>first_update_id</th>\n",
       "      <th>last_update_id</th>\n",
       "      <th>side</th>\n",
       "      <th>update_type</th>\n",
       "      <th>price</th>\n",
       "      <th>qty</th>\n",
       "    </tr>\n",
       "  </thead>\n",
       "  <tbody>\n",
       "    <tr>\n",
       "      <th>0</th>\n",
       "      <td>BTCUSDT</td>\n",
       "      <td>1714953565827</td>\n",
       "      <td>1714953565827866380</td>\n",
       "      <td>4556869216794</td>\n",
       "      <td>4556869216794</td>\n",
       "      <td>b</td>\n",
       "      <td>set</td>\n",
       "      <td>63930.7</td>\n",
       "      <td>0.444</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>1</th>\n",
       "      <td>BTCUSDT</td>\n",
       "      <td>1714953565831</td>\n",
       "      <td>1714953565831574540</td>\n",
       "      <td>4556869216931</td>\n",
       "      <td>4556869216931</td>\n",
       "      <td>a</td>\n",
       "      <td>set</td>\n",
       "      <td>64009.1</td>\n",
       "      <td>0.236</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>2</th>\n",
       "      <td>BTCUSDT</td>\n",
       "      <td>1714953565833</td>\n",
       "      <td>1714953565833602752</td>\n",
       "      <td>4556869217011</td>\n",
       "      <td>4556869217011</td>\n",
       "      <td>b</td>\n",
       "      <td>set</td>\n",
       "      <td>63971.1</td>\n",
       "      <td>2.744</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>3</th>\n",
       "      <td>BTCUSDT</td>\n",
       "      <td>1714953565834</td>\n",
       "      <td>1714953565834150447</td>\n",
       "      <td>4556869217023</td>\n",
       "      <td>4556869217023</td>\n",
       "      <td>b</td>\n",
       "      <td>set</td>\n",
       "      <td>63968.0</td>\n",
       "      <td>0.118</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>4</th>\n",
       "      <td>BTCUSDT</td>\n",
       "      <td>1714953565835</td>\n",
       "      <td>1714953565835679966</td>\n",
       "      <td>4556869217064</td>\n",
       "      <td>4556869217064</td>\n",
       "      <td>a</td>\n",
       "      <td>set</td>\n",
       "      <td>64002.4</td>\n",
       "      <td>1.047</td>\n",
       "    </tr>\n",
       "  </tbody>\n",
       "</table>\n",
       "</div>"
      ],
      "text/plain": [
       "    symbol      timestamp             trans_id  first_update_id  last_update_id side update_type    price    qty\n",
       "0  BTCUSDT  1714953565827  1714953565827866380    4556869216794   4556869216794    b         set  63930.7  0.444\n",
       "1  BTCUSDT  1714953565831  1714953565831574540    4556869216931   4556869216931    a         set  64009.1  0.236\n",
       "2  BTCUSDT  1714953565833  1714953565833602752    4556869217011   4556869217011    b         set  63971.1  2.744\n",
       "3  BTCUSDT  1714953565834  1714953565834150447    4556869217023   4556869217023    b         set  63968.0  0.118\n",
       "4  BTCUSDT  1714953565835  1714953565835679966    4556869217064   4556869217064    a         set  64002.4  1.047"
      ]
     },
     "execution_count": 108,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "update_csv.head()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 109,
   "id": "cee36a16",
   "metadata": {
    "ExecuteTime": {
     "end_time": "2024-06-20T19:52:59.699084Z",
     "start_time": "2024-06-20T19:52:59.671924Z"
    }
   },
   "outputs": [
    {
     "data": {
      "text/plain": [
       "array(['set', 's'], dtype=object)"
      ]
     },
     "execution_count": 109,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "update_csv[\"update_type\"].unique()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 110,
   "id": "df0f4028",
   "metadata": {
    "ExecuteTime": {
     "end_time": "2024-06-20T19:53:00.945151Z",
     "start_time": "2024-06-20T19:53:00.940158Z"
    }
   },
   "outputs": [
    {
     "data": {
      "text/plain": [
       "Timestamp('2024-05-05 23:59:25.827000')"
      ]
     },
     "execution_count": 110,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "pd.to_datetime(update_csv.timestamp.min(), unit=\"ms\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 111,
   "id": "f779e771",
   "metadata": {
    "ExecuteTime": {
     "end_time": "2024-06-20T19:53:01.079393Z",
     "start_time": "2024-06-20T19:53:01.073292Z"
    }
   },
   "outputs": [
    {
     "data": {
      "text/plain": [
       "Timestamp('2024-05-06 00:06:35.690000')"
      ]
     },
     "execution_count": 111,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "end_timestamp = pd.to_datetime(update_csv.timestamp.max(), unit=\"ms\")\n",
    "end_timestamp"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "fb34b965",
   "metadata": {},
   "source": [
    "## Load our data\n",
    "\n",
    "We will use archived data from our realtime websocket collection script\n",
    "- each timestamp contain information about top of the book"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 112,
   "id": "303f7758",
   "metadata": {
    "ExecuteTime": {
     "end_time": "2024-06-20T19:53:51.272187Z",
     "start_time": "2024-06-20T19:53:01.774328Z"
    }
   },
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "INFO  Loading dataset schema file: /app/amp/data_schema/dataset_schema_versions/dataset_schema_v3.json\n",
      "INFO  Loaded dataset schema version v3\n",
      "INFO  Loading dataset schema file: /app/amp/data_schema/dataset_schema_versions/dataset_schema_v3.json\n",
      "INFO  Loaded dataset schema version v3\n",
      "INFO  Loading dataset schema file: /app/amp/data_schema/dataset_schema_versions/dataset_schema_v3.json\n",
      "INFO  Loaded dataset schema version v3\n"
     ]
    },
    {
     "data": {
      "text/html": [
       "<div>\n",
       "<style scoped>\n",
       "    .dataframe tbody tr th:only-of-type {\n",
       "        vertical-align: middle;\n",
       "    }\n",
       "\n",
       "    .dataframe tbody tr th {\n",
       "        vertical-align: top;\n",
       "    }\n",
       "\n",
       "    .dataframe thead th {\n",
       "        text-align: right;\n",
       "    }\n",
       "</style>\n",
       "<table border=\"1\" class=\"dataframe\">\n",
       "  <thead>\n",
       "    <tr style=\"text-align: right;\">\n",
       "      <th></th>\n",
       "      <th>timestamp</th>\n",
       "      <th>bid_size</th>\n",
       "      <th>bid_price</th>\n",
       "      <th>ask_size</th>\n",
       "      <th>ask_price</th>\n",
       "      <th>exchange_id</th>\n",
       "      <th>level</th>\n",
       "      <th>end_download_timestamp</th>\n",
       "      <th>knowledge_timestamp</th>\n",
       "      <th>currency_pair</th>\n",
       "      <th>year</th>\n",
       "      <th>month</th>\n",
       "      <th>day</th>\n",
       "    </tr>\n",
       "  </thead>\n",
       "  <tbody>\n",
       "    <tr>\n",
       "      <th>2024-05-05 23:59:26.686000+00:00</th>\n",
       "      <td>1714953566686</td>\n",
       "      <td>5.303</td>\n",
       "      <td>63983.9</td>\n",
       "      <td>0.564</td>\n",
       "      <td>63984.0</td>\n",
       "      <td>binance</td>\n",
       "      <td>1</td>\n",
       "      <td>2024-05-05 23:59:26.728295+00:00</td>\n",
       "      <td>2024-05-05 23:59:26.743708+00:00</td>\n",
       "      <td>BTC_USDT</td>\n",
       "      <td>2024</td>\n",
       "      <td>5</td>\n",
       "      <td>5</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>2024-05-05 23:59:26.838000+00:00</th>\n",
       "      <td>1714953566838</td>\n",
       "      <td>5.228</td>\n",
       "      <td>63983.9</td>\n",
       "      <td>0.564</td>\n",
       "      <td>63984.0</td>\n",
       "      <td>binance</td>\n",
       "      <td>1</td>\n",
       "      <td>2024-05-05 23:59:26.930693+00:00</td>\n",
       "      <td>2024-05-05 23:59:26.945667+00:00</td>\n",
       "      <td>BTC_USDT</td>\n",
       "      <td>2024</td>\n",
       "      <td>5</td>\n",
       "      <td>5</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>2024-05-05 23:59:27.104000+00:00</th>\n",
       "      <td>1714953567104</td>\n",
       "      <td>1.055</td>\n",
       "      <td>63983.9</td>\n",
       "      <td>4.873</td>\n",
       "      <td>63984.0</td>\n",
       "      <td>binance</td>\n",
       "      <td>1</td>\n",
       "      <td>2024-05-05 23:59:27.132223+00:00</td>\n",
       "      <td>2024-05-05 23:59:27.148182+00:00</td>\n",
       "      <td>BTC_USDT</td>\n",
       "      <td>2024</td>\n",
       "      <td>5</td>\n",
       "      <td>5</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th></th>\n",
       "      <td>...</td>\n",
       "      <td>...</td>\n",
       "      <td>...</td>\n",
       "      <td>...</td>\n",
       "      <td>...</td>\n",
       "      <td>...</td>\n",
       "      <td>...</td>\n",
       "      <td>...</td>\n",
       "      <td>...</td>\n",
       "      <td>...</td>\n",
       "      <td>...</td>\n",
       "      <td>...</td>\n",
       "      <td>...</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>2024-05-06 00:06:35.506000+00:00</th>\n",
       "      <td>1714953995506</td>\n",
       "      <td>1.457</td>\n",
       "      <td>64044.6</td>\n",
       "      <td>7.715</td>\n",
       "      <td>64044.7</td>\n",
       "      <td>binance</td>\n",
       "      <td>1</td>\n",
       "      <td>2024-05-06 00:06:35.548971+00:00</td>\n",
       "      <td>2024-05-06 00:06:35.590357+00:00</td>\n",
       "      <td>BTC_USDT</td>\n",
       "      <td>2024</td>\n",
       "      <td>5</td>\n",
       "      <td>6</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>2024-05-06 00:06:35.204000+00:00</th>\n",
       "      <td>1714953995204</td>\n",
       "      <td>3.027</td>\n",
       "      <td>64050.0</td>\n",
       "      <td>1.515</td>\n",
       "      <td>64050.1</td>\n",
       "      <td>binance</td>\n",
       "      <td>1</td>\n",
       "      <td>2024-05-06 00:06:35.311070+00:00</td>\n",
       "      <td>2024-05-06 00:06:35.344582+00:00</td>\n",
       "      <td>BTC_USDT</td>\n",
       "      <td>2024</td>\n",
       "      <td>5</td>\n",
       "      <td>6</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>2024-05-06 00:06:35.608000+00:00</th>\n",
       "      <td>1714953995608</td>\n",
       "      <td>0.081</td>\n",
       "      <td>64044.1</td>\n",
       "      <td>13.408</td>\n",
       "      <td>64044.2</td>\n",
       "      <td>binance</td>\n",
       "      <td>1</td>\n",
       "      <td>2024-05-06 00:06:35.715801+00:00</td>\n",
       "      <td>2024-05-06 00:06:35.741555+00:00</td>\n",
       "      <td>BTC_USDT</td>\n",
       "      <td>2024</td>\n",
       "      <td>5</td>\n",
       "      <td>6</td>\n",
       "    </tr>\n",
       "  </tbody>\n",
       "</table>\n",
       "</div>"
      ],
      "text/plain": [
       "                                      timestamp bid_size bid_price ask_size ask_price exchange_id level            end_download_timestamp               knowledge_timestamp currency_pair  year month  day\n",
       "2024-05-05 23:59:26.686000+00:00  1714953566686    5.303   63983.9    0.564   63984.0     binance     1  2024-05-05 23:59:26.728295+00:00  2024-05-05 23:59:26.743708+00:00      BTC_USDT  2024     5    5\n",
       "2024-05-05 23:59:26.838000+00:00  1714953566838    5.228   63983.9    0.564   63984.0     binance     1  2024-05-05 23:59:26.930693+00:00  2024-05-05 23:59:26.945667+00:00      BTC_USDT  2024     5    5\n",
       "2024-05-05 23:59:27.104000+00:00  1714953567104    1.055   63983.9    4.873   63984.0     binance     1  2024-05-05 23:59:27.132223+00:00  2024-05-05 23:59:27.148182+00:00      BTC_USDT  2024     5    5\n",
       "                                            ...      ...       ...      ...       ...         ...   ...                               ...                               ...           ...   ...   ...  ...\n",
       "2024-05-06 00:06:35.506000+00:00  1714953995506    1.457   64044.6    7.715   64044.7     binance     1  2024-05-06 00:06:35.548971+00:00  2024-05-06 00:06:35.590357+00:00      BTC_USDT  2024     5    6\n",
       "2024-05-06 00:06:35.204000+00:00  1714953995204    3.027   64050.0    1.515   64050.1     binance     1  2024-05-06 00:06:35.311070+00:00  2024-05-06 00:06:35.344582+00:00      BTC_USDT  2024     5    6\n",
       "2024-05-06 00:06:35.608000+00:00  1714953995608    0.081   64044.1   13.408   64044.2     binance     1  2024-05-06 00:06:35.715801+00:00  2024-05-06 00:06:35.741555+00:00      BTC_USDT  2024     5    6"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "INFO  None\n"
     ]
    }
   ],
   "source": [
    "signature = \"periodic_daily.airflow.archived_200ms.parquet.bid_ask.futures.v8.binance.binance.v1_0_0\"\n",
    "reader = imvcdcimrdc.RawDataReader(signature, stage=\"preprod\", add_suffix=\"tokyo\")\n",
    "data = reader.read_data(\n",
    "    start_timestamp, end_timestamp, currency_pairs=[\"BTC_USDT\"]\n",
    ")\n",
    "_LOG.log(log_level, hpandas.df_to_str(data, log_level=log_level))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 113,
   "id": "d11ab0a9",
   "metadata": {
    "ExecuteTime": {
     "end_time": "2024-06-20T19:53:51.279202Z",
     "start_time": "2024-06-20T19:53:51.274872Z"
    }
   },
   "outputs": [
    {
     "data": {
      "text/plain": [
       "(2455, 13)"
      ]
     },
     "execution_count": 113,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "data.shape"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 114,
   "id": "b755a92d",
   "metadata": {
    "ExecuteTime": {
     "end_time": "2024-06-20T19:53:51.283500Z",
     "start_time": "2024-06-20T19:53:51.280799Z"
    }
   },
   "outputs": [],
   "source": [
    "archived_200ms_data = data"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "057c5d25",
   "metadata": {},
   "source": [
    "## Transform tick-by-tick data to snapshots"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "6420742e",
   "metadata": {},
   "source": [
    "Confirm all timestamp from snapshost are present in tick by tick data"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 115,
   "id": "07bd1dd9",
   "metadata": {
    "ExecuteTime": {
     "end_time": "2024-06-20T19:53:51.290316Z",
     "start_time": "2024-06-20T19:53:51.286405Z"
    }
   },
   "outputs": [
    {
     "data": {
      "text/plain": [
       "2455"
      ]
     },
     "execution_count": 115,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "len(set(archived_200ms_data.timestamp))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 116,
   "id": "1fbc41f1",
   "metadata": {
    "ExecuteTime": {
     "end_time": "2024-06-20T19:53:51.406468Z",
     "start_time": "2024-06-20T19:53:51.293247Z"
    }
   },
   "outputs": [
    {
     "data": {
      "text/plain": [
       "176567"
      ]
     },
     "execution_count": 116,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "len(set(update_csv.timestamp))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 117,
   "id": "fc75883f",
   "metadata": {
    "ExecuteTime": {
     "end_time": "2024-06-20T19:53:51.491408Z",
     "start_time": "2024-06-20T19:53:51.408668Z"
    }
   },
   "outputs": [
    {
     "data": {
      "text/plain": [
       "set()"
      ]
     },
     "execution_count": 117,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "set(archived_200ms_data.timestamp) - set(update_csv.timestamp)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 146,
   "id": "f5f23337",
   "metadata": {
    "ExecuteTime": {
     "end_time": "2024-06-20T20:04:48.057656Z",
     "start_time": "2024-06-20T20:04:48.044483Z"
    }
   },
   "outputs": [],
   "source": [
    "def transform_tick_by_tick_to_snapshot(tick_by_tick_df: pd.DataFrame, snapshot_df: pd.DataFrame) -> pd.DataFrame:\n",
    "    \"\"\"\n",
    "    TODO(Juraj): Add docstring\n",
    "    \"\"\"\n",
    "    reconstructed_df = []\n",
    "    \n",
    "    for i, row in tick_by_tick_df.iterrows():\n",
    "        curr_side = row[\"side\"]\n",
    "        if row[\"price\"] in _ORDER_BOOK_SNAPSHOT[curr_side]:\n",
    "            if row[\"qty\"] == 0:\n",
    "                _ORDER_BOOK_SNAPSHOT[curr_side].pop(row[\"price\"])\n",
    "            else:\n",
    "                _ORDER_BOOK_SNAPSHOT[curr_side][row[\"price\"]] = row[\"qty\"]\n",
    "        else:\n",
    "            if row[\"qty\"] != 0:\n",
    "                _ORDER_BOOK_SNAPSHOT[curr_side][row[\"price\"]] = row[\"qty\"]\n",
    "                if len(_ORDER_BOOK_SNAPSHOT[curr_side]) > 250:\n",
    "                    _ORDER_BOOK_SNAPSHOT[curr_side].popitem(-1)\n",
    "                \n",
    "        if row[\"timestamp\"] in set(snapshot_df.timestamp):\n",
    "            reconstructed_df.append({\n",
    "                'timestamp': row[\"timestamp\"],\n",
    "                'bid_size': _ORDER_BOOK_SNAPSHOT[\"b\"].peekitem(0)[1],\n",
    "                'bid_price': _ORDER_BOOK_SNAPSHOT[\"b\"].peekitem(0)[0],\n",
    "                'ask_size': _ORDER_BOOK_SNAPSHOT[\"a\"].peekitem(0)[1],\n",
    "                'ask_price': _ORDER_BOOK_SNAPSHOT[\"a\"].peekitem(0)[0]\n",
    "            })\n",
    "    return pd.DataFrame(reconstructed_df)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "9879c60b",
   "metadata": {
    "ExecuteTime": {
     "end_time": "2024-06-20T20:18:54.011525Z",
     "start_time": "2024-06-20T20:04:49.043192Z"
    }
   },
   "outputs": [],
   "source": [
    "transformed_df = transform_tick_by_tick_to_snapshot(update_csv, archived_200ms_data)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 148,
   "id": "59c691a7",
   "metadata": {
    "ExecuteTime": {
     "end_time": "2024-06-20T20:21:33.291419Z",
     "start_time": "2024-06-20T20:21:33.257806Z"
    }
   },
   "outputs": [],
   "source": [
    "transformed_df.to_csv(f\"/app/{symbol}_T_DEPTH_2024-05-06_transformed.csv\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 150,
   "id": "e39c2d59",
   "metadata": {
    "ExecuteTime": {
     "end_time": "2024-06-20T20:22:09.601116Z",
     "start_time": "2024-06-20T20:22:09.588907Z"
    }
   },
   "outputs": [
    {
     "data": {
      "text/html": [
       "<div>\n",
       "<style scoped>\n",
       "    .dataframe tbody tr th:only-of-type {\n",
       "        vertical-align: middle;\n",
       "    }\n",
       "\n",
       "    .dataframe tbody tr th {\n",
       "        vertical-align: top;\n",
       "    }\n",
       "\n",
       "    .dataframe thead th {\n",
       "        text-align: right;\n",
       "    }\n",
       "</style>\n",
       "<table border=\"1\" class=\"dataframe\">\n",
       "  <thead>\n",
       "    <tr style=\"text-align: right;\">\n",
       "      <th></th>\n",
       "      <th>timestamp</th>\n",
       "      <th>bid_size</th>\n",
       "      <th>bid_price</th>\n",
       "      <th>ask_size</th>\n",
       "      <th>ask_price</th>\n",
       "    </tr>\n",
       "  </thead>\n",
       "  <tbody>\n",
       "    <tr>\n",
       "      <th>0</th>\n",
       "      <td>1714953565849</td>\n",
       "      <td>5.140</td>\n",
       "      <td>63983.9</td>\n",
       "      <td>1.115</td>\n",
       "      <td>63984.0</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>1</th>\n",
       "      <td>1714953565849</td>\n",
       "      <td>5.140</td>\n",
       "      <td>63983.9</td>\n",
       "      <td>1.115</td>\n",
       "      <td>63984.0</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>2</th>\n",
       "      <td>1714953565849</td>\n",
       "      <td>5.140</td>\n",
       "      <td>63983.9</td>\n",
       "      <td>1.115</td>\n",
       "      <td>63984.0</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>3</th>\n",
       "      <td>1714953566108</td>\n",
       "      <td>5.213</td>\n",
       "      <td>63983.9</td>\n",
       "      <td>1.086</td>\n",
       "      <td>63984.0</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>4</th>\n",
       "      <td>1714953566218</td>\n",
       "      <td>5.279</td>\n",
       "      <td>63983.9</td>\n",
       "      <td>1.086</td>\n",
       "      <td>63984.0</td>\n",
       "    </tr>\n",
       "  </tbody>\n",
       "</table>\n",
       "</div>"
      ],
      "text/plain": [
       "       timestamp  bid_size  bid_price  ask_size  ask_price\n",
       "0  1714953565849     5.140    63983.9     1.115    63984.0\n",
       "1  1714953565849     5.140    63983.9     1.115    63984.0\n",
       "2  1714953565849     5.140    63983.9     1.115    63984.0\n",
       "3  1714953566108     5.213    63983.9     1.086    63984.0\n",
       "4  1714953566218     5.279    63983.9     1.086    63984.0"
      ]
     },
     "execution_count": 150,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "transformed_df.head()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 152,
   "id": "89acad17",
   "metadata": {
    "ExecuteTime": {
     "end_time": "2024-06-20T20:24:35.835612Z",
     "start_time": "2024-06-20T20:24:35.832191Z"
    }
   },
   "outputs": [],
   "source": [
    "# Timestamp is also in column, will be easier to merge with the other dataset\n",
    "archived_200ms_data = archived_200ms_data.reset_index(drop=True)"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "54e2cdcb",
   "metadata": {},
   "source": [
    "## Compare the data"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 155,
   "id": "f919cfc8",
   "metadata": {
    "ExecuteTime": {
     "end_time": "2024-06-20T20:25:18.097612Z",
     "start_time": "2024-06-20T20:25:18.084704Z"
    }
   },
   "outputs": [],
   "source": [
    "# Merge the reconstructed data with the third dataset based on timestamp\n",
    "merged_df = pd.merge(transformed_df, archived_200ms_data, on='timestamp', suffixes=('_reconstructed', '_rt_archived'))\n",
    "\n",
    "# Calculate the deviation percentage for each column\n",
    "for column in ['bid_size', 'bid_price', 'ask_size', 'ask_price']:\n",
    "    merged_df[f'{column}_deviation'] = abs(merged_df[f'{column}_reconstructed'] - merged_df[f'{column}_rt_archived']) / merged_df[f'{column}_rt_archived'] * 100\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 156,
   "id": "a0bd5eac",
   "metadata": {
    "ExecuteTime": {
     "end_time": "2024-06-20T20:25:25.367023Z",
     "start_time": "2024-06-20T20:25:25.347304Z"
    }
   },
   "outputs": [
    {
     "data": {
      "text/html": [
       "<div>\n",
       "<style scoped>\n",
       "    .dataframe tbody tr th:only-of-type {\n",
       "        vertical-align: middle;\n",
       "    }\n",
       "\n",
       "    .dataframe tbody tr th {\n",
       "        vertical-align: top;\n",
       "    }\n",
       "\n",
       "    .dataframe thead th {\n",
       "        text-align: right;\n",
       "    }\n",
       "</style>\n",
       "<table border=\"1\" class=\"dataframe\">\n",
       "  <thead>\n",
       "    <tr style=\"text-align: right;\">\n",
       "      <th></th>\n",
       "      <th>timestamp</th>\n",
       "      <th>bid_size_reconstructed</th>\n",
       "      <th>bid_price_reconstructed</th>\n",
       "      <th>ask_size_reconstructed</th>\n",
       "      <th>ask_price_reconstructed</th>\n",
       "      <th>bid_size_rt_archived</th>\n",
       "      <th>bid_price_rt_archived</th>\n",
       "      <th>ask_size_rt_archived</th>\n",
       "      <th>ask_price_rt_archived</th>\n",
       "      <th>exchange_id</th>\n",
       "      <th>level</th>\n",
       "      <th>end_download_timestamp</th>\n",
       "      <th>knowledge_timestamp</th>\n",
       "      <th>currency_pair</th>\n",
       "      <th>year</th>\n",
       "      <th>month</th>\n",
       "      <th>day</th>\n",
       "      <th>bid_size_deviation</th>\n",
       "      <th>bid_price_deviation</th>\n",
       "      <th>ask_size_deviation</th>\n",
       "      <th>ask_price_deviation</th>\n",
       "    </tr>\n",
       "  </thead>\n",
       "  <tbody>\n",
       "    <tr>\n",
       "      <th>0</th>\n",
       "      <td>1714953565849</td>\n",
       "      <td>5.140</td>\n",
       "      <td>63983.9</td>\n",
       "      <td>1.115</td>\n",
       "      <td>63984.0</td>\n",
       "      <td>5.140</td>\n",
       "      <td>63983.9</td>\n",
       "      <td>1.115</td>\n",
       "      <td>63984.0</td>\n",
       "      <td>binance</td>\n",
       "      <td>1</td>\n",
       "      <td>2024-05-05 23:59:25.923611+00:00</td>\n",
       "      <td>2024-05-05 23:59:25.938259+00:00</td>\n",
       "      <td>BTC_USDT</td>\n",
       "      <td>2024</td>\n",
       "      <td>5</td>\n",
       "      <td>5</td>\n",
       "      <td>0.0</td>\n",
       "      <td>0.0</td>\n",
       "      <td>0.0</td>\n",
       "      <td>0.0</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>1</th>\n",
       "      <td>1714953565849</td>\n",
       "      <td>5.140</td>\n",
       "      <td>63983.9</td>\n",
       "      <td>1.115</td>\n",
       "      <td>63984.0</td>\n",
       "      <td>5.140</td>\n",
       "      <td>63983.9</td>\n",
       "      <td>1.115</td>\n",
       "      <td>63984.0</td>\n",
       "      <td>binance</td>\n",
       "      <td>1</td>\n",
       "      <td>2024-05-05 23:59:25.923611+00:00</td>\n",
       "      <td>2024-05-05 23:59:25.938259+00:00</td>\n",
       "      <td>BTC_USDT</td>\n",
       "      <td>2024</td>\n",
       "      <td>5</td>\n",
       "      <td>5</td>\n",
       "      <td>0.0</td>\n",
       "      <td>0.0</td>\n",
       "      <td>0.0</td>\n",
       "      <td>0.0</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>2</th>\n",
       "      <td>1714953565849</td>\n",
       "      <td>5.140</td>\n",
       "      <td>63983.9</td>\n",
       "      <td>1.115</td>\n",
       "      <td>63984.0</td>\n",
       "      <td>5.140</td>\n",
       "      <td>63983.9</td>\n",
       "      <td>1.115</td>\n",
       "      <td>63984.0</td>\n",
       "      <td>binance</td>\n",
       "      <td>1</td>\n",
       "      <td>2024-05-05 23:59:25.923611+00:00</td>\n",
       "      <td>2024-05-05 23:59:25.938259+00:00</td>\n",
       "      <td>BTC_USDT</td>\n",
       "      <td>2024</td>\n",
       "      <td>5</td>\n",
       "      <td>5</td>\n",
       "      <td>0.0</td>\n",
       "      <td>0.0</td>\n",
       "      <td>0.0</td>\n",
       "      <td>0.0</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>3</th>\n",
       "      <td>1714953566108</td>\n",
       "      <td>5.213</td>\n",
       "      <td>63983.9</td>\n",
       "      <td>1.086</td>\n",
       "      <td>63984.0</td>\n",
       "      <td>5.213</td>\n",
       "      <td>63983.9</td>\n",
       "      <td>1.086</td>\n",
       "      <td>63984.0</td>\n",
       "      <td>binance</td>\n",
       "      <td>1</td>\n",
       "      <td>2024-05-05 23:59:26.120215+00:00</td>\n",
       "      <td>2024-05-05 23:59:26.142422+00:00</td>\n",
       "      <td>BTC_USDT</td>\n",
       "      <td>2024</td>\n",
       "      <td>5</td>\n",
       "      <td>5</td>\n",
       "      <td>0.0</td>\n",
       "      <td>0.0</td>\n",
       "      <td>0.0</td>\n",
       "      <td>0.0</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>4</th>\n",
       "      <td>1714953566218</td>\n",
       "      <td>5.279</td>\n",
       "      <td>63983.9</td>\n",
       "      <td>1.086</td>\n",
       "      <td>63984.0</td>\n",
       "      <td>5.279</td>\n",
       "      <td>63983.9</td>\n",
       "      <td>1.086</td>\n",
       "      <td>63984.0</td>\n",
       "      <td>binance</td>\n",
       "      <td>1</td>\n",
       "      <td>2024-05-05 23:59:26.325117+00:00</td>\n",
       "      <td>2024-05-05 23:59:26.341712+00:00</td>\n",
       "      <td>BTC_USDT</td>\n",
       "      <td>2024</td>\n",
       "      <td>5</td>\n",
       "      <td>5</td>\n",
       "      <td>0.0</td>\n",
       "      <td>0.0</td>\n",
       "      <td>0.0</td>\n",
       "      <td>0.0</td>\n",
       "    </tr>\n",
       "  </tbody>\n",
       "</table>\n",
       "</div>"
      ],
      "text/plain": [
       "       timestamp  bid_size_reconstructed  bid_price_reconstructed  ask_size_reconstructed  ask_price_reconstructed  bid_size_rt_archived  bid_price_rt_archived  ask_size_rt_archived  ask_price_rt_archived exchange_id  level           end_download_timestamp              knowledge_timestamp currency_pair  year  month  day  bid_size_deviation  bid_price_deviation  ask_size_deviation  ask_price_deviation\n",
       "0  1714953565849                   5.140                  63983.9                   1.115                  63984.0                 5.140                63983.9                 1.115                63984.0     binance      1 2024-05-05 23:59:25.923611+00:00 2024-05-05 23:59:25.938259+00:00      BTC_USDT  2024      5    5                 0.0                  0.0                 0.0                  0.0\n",
       "1  1714953565849                   5.140                  63983.9                   1.115                  63984.0                 5.140                63983.9                 1.115                63984.0     binance      1 2024-05-05 23:59:25.923611+00:00 2024-05-05 23:59:25.938259+00:00      BTC_USDT  2024      5    5                 0.0                  0.0                 0.0                  0.0\n",
       "2  1714953565849                   5.140                  63983.9                   1.115                  63984.0                 5.140                63983.9                 1.115                63984.0     binance      1 2024-05-05 23:59:25.923611+00:00 2024-05-05 23:59:25.938259+00:00      BTC_USDT  2024      5    5                 0.0                  0.0                 0.0                  0.0\n",
       "3  1714953566108                   5.213                  63983.9                   1.086                  63984.0                 5.213                63983.9                 1.086                63984.0     binance      1 2024-05-05 23:59:26.120215+00:00 2024-05-05 23:59:26.142422+00:00      BTC_USDT  2024      5    5                 0.0                  0.0                 0.0                  0.0\n",
       "4  1714953566218                   5.279                  63983.9                   1.086                  63984.0                 5.279                63983.9                 1.086                63984.0     binance      1 2024-05-05 23:59:26.325117+00:00 2024-05-05 23:59:26.341712+00:00      BTC_USDT  2024      5    5                 0.0                  0.0                 0.0                  0.0"
      ]
     },
     "execution_count": 156,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "merged_df.head()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 159,
   "id": "2df14401",
   "metadata": {
    "ExecuteTime": {
     "end_time": "2024-06-20T20:27:22.724683Z",
     "start_time": "2024-06-20T20:27:22.700644Z"
    }
   },
   "outputs": [
    {
     "data": {
      "text/html": [
       "<div>\n",
       "<style scoped>\n",
       "    .dataframe tbody tr th:only-of-type {\n",
       "        vertical-align: middle;\n",
       "    }\n",
       "\n",
       "    .dataframe tbody tr th {\n",
       "        vertical-align: top;\n",
       "    }\n",
       "\n",
       "    .dataframe thead th {\n",
       "        text-align: right;\n",
       "    }\n",
       "</style>\n",
       "<table border=\"1\" class=\"dataframe\">\n",
       "  <thead>\n",
       "    <tr style=\"text-align: right;\">\n",
       "      <th></th>\n",
       "      <th>bid_size_deviation</th>\n",
       "      <th>bid_price_deviation</th>\n",
       "      <th>ask_size_deviation</th>\n",
       "      <th>ask_price_deviation</th>\n",
       "    </tr>\n",
       "  </thead>\n",
       "  <tbody>\n",
       "    <tr>\n",
       "      <th>count</th>\n",
       "      <td>7023.000000</td>\n",
       "      <td>7023.000000</td>\n",
       "      <td>7023.000000</td>\n",
       "      <td>7023.000000</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>mean</th>\n",
       "      <td>22.420080</td>\n",
       "      <td>0.000125</td>\n",
       "      <td>15.287032</td>\n",
       "      <td>0.000114</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>std</th>\n",
       "      <td>277.873941</td>\n",
       "      <td>0.000951</td>\n",
       "      <td>216.252026</td>\n",
       "      <td>0.000977</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>min</th>\n",
       "      <td>0.000000</td>\n",
       "      <td>0.000000</td>\n",
       "      <td>0.000000</td>\n",
       "      <td>0.000000</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>25%</th>\n",
       "      <td>0.000000</td>\n",
       "      <td>0.000000</td>\n",
       "      <td>0.000000</td>\n",
       "      <td>0.000000</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>50%</th>\n",
       "      <td>0.000000</td>\n",
       "      <td>0.000000</td>\n",
       "      <td>0.000000</td>\n",
       "      <td>0.000000</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>75%</th>\n",
       "      <td>0.000000</td>\n",
       "      <td>0.000000</td>\n",
       "      <td>0.000000</td>\n",
       "      <td>0.000000</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>max</th>\n",
       "      <td>6450.000000</td>\n",
       "      <td>0.009989</td>\n",
       "      <td>4450.000000</td>\n",
       "      <td>0.015764</td>\n",
       "    </tr>\n",
       "  </tbody>\n",
       "</table>\n",
       "</div>"
      ],
      "text/plain": [
       "       bid_size_deviation  bid_price_deviation  ask_size_deviation  ask_price_deviation\n",
       "count         7023.000000          7023.000000         7023.000000          7023.000000\n",
       "mean            22.420080             0.000125           15.287032             0.000114\n",
       "std            277.873941             0.000951          216.252026             0.000977\n",
       "min              0.000000             0.000000            0.000000             0.000000\n",
       "25%              0.000000             0.000000            0.000000             0.000000\n",
       "50%              0.000000             0.000000            0.000000             0.000000\n",
       "75%              0.000000             0.000000            0.000000             0.000000\n",
       "max           6450.000000             0.009989         4450.000000             0.015764"
      ]
     },
     "execution_count": 159,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "merged_df[[\"bid_size_deviation\", \"bid_price_deviation\", \"ask_size_deviation\", \"ask_price_deviation\"]].describe()"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "02bbc39b",
   "metadata": {},
   "source": [
    "The conclusion is that we are able to match the orderbook snapshost to the tick by tick history reasonably well."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "f4087cf1",
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.9.5"
  },
  "toc": {
   "base_numbering": 1,
   "nav_menu": {},
   "number_sections": true,
   "sideBar": true,
   "skip_h1_title": false,
   "title_cell": "Table of Contents",
   "title_sidebar": "Contents",
   "toc_cell": false,
   "toc_position": {},
   "toc_section_display": true,
   "toc_window_display": false
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
