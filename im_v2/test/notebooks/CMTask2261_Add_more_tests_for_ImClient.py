# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.13.8
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Imports

# %%
import logging

import pandas as pd

import core.config.config_ as cconconf
import core.config.config_utils as ccocouti
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hprint as hprint
import im_v2.ccxt.data.client.ccxt_clients_example as imvcdcccex
import im_v2.crypto_chassis.data.client.crypto_chassis_clients_example as imvccdcccce
import im_v2.common.data.client.test.im_client_test_case as icdctictc

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

hprint.config_notebook()


# %% [markdown]
# # Config

# %%
def get_config():
    config = cconconf.Config()
    param_dict = {
        "im_client_params": {
            "ccxt_params": {
                "universe_version": "v4",
                "resample_1min": False,
                "dataset": "ohlcv",
                "contract_type": "spot",
                "data_snapshot": "20220530"
            },
            "crypto_chassis_params": {
                "bid_ask": {
                    "universe_version": "v3",
                    "resample_1min": True,
                    "dataset": "bid_ask",
                    "contract_type": "futures",
                    "data_snapshot": "20220620"
            },
                "ohlcv": {
                    "universe_version": "v3",
                    "resample_1min": True,
                    "dataset": "ohlcv",
                    "contract_type": "spot",
                    "data_snapshot": "20220530"
                }
            }
        },
        "read_data": {
            "full_symbols": ["binance::BTC_USDT", "binance::ADA_USDT"],
            "start_ts": pd.Timestamp("2022-05-15 13:00:00+00:00"),
            "end_ts": pd.Timestamp("2022-05-15 13:10:00+00:00"),
        }
    }
    config = ccocouti.get_config_from_nested_dict(param_dict)
    return config
# 
config = get_config()
print(config)


# %% [markdown]
# # Function

# %%
def test_read_data(
    im_client,
    **expected_values,
):
    im_client_test_case  = icdctictc.ImClientTestCase()
    im_client_test_case._test_read_data5(
        im_client,
        **expected_values,
    )


# %% [markdown]
# # CcxtHistoricalPqByTileClient

# %% run_control={"marked": true}
ccxt_historical_client = imvcdcccex.get_CcxtHistoricalPqByTileClient_example1(
    **config["im_client_params"]["ccxt_params"]
)
# TODO(Nina): DO NOT FORGET TO EDIT OUTPUTS
expected = {
    "expected_signature": r"""# df=
        index=[2022-05-15 13:00:00+00:00, 2022-05-15 13:15:00+00:00]
        columns=full_symbol,open,high,low,close,volume,vwap,number_of_trades,twap,knowledge_timestamp
        shape=(32, 10)
                                          full_symbol        open        high         low       close        volume          vwap  number_of_trades          twap              knowledge_timestamp
        timestamp
        2022-05-15 13:00:00+00:00   binance::BTC_USDT  30309.9900  30315.9100  30280.0000  30280.9500      45.13335  30291.409100               607  30291.172400 2022-05-31 14:38:23.953697+00:00
        2022-05-15 13:00:00+00:00  binance::DOGE_USDT      0.0895      0.0895      0.0894      0.0894  606842.00000      0.089435                49      0.089469 2022-05-31 14:38:29.472387+00:00
        2022-05-15 13:01:00+00:00   binance::BTC_USDT  30280.9500  30299.5700  30263.2800  30263.3100      39.45772  30278.419700               577  30278.335000 2022-05-31 14:38:23.953697+00:00
        ...
        2022-05-15 13:14:00+00:00  binance::DOGE_USDT      0.0895      0.0897      0.0894      0.0896  233260.00000      0.089547                30      0.089507 2022-05-31 14:38:29.472387+00:00
        2022-05-15 13:15:00+00:00   binance::BTC_USDT  30363.5000  30400.0000  30324.2300  30387.2600      62.71008  30370.925000              1364  30374.435700 2022-05-31 14:38:23.953697+00:00
        2022-05-15 13:15:00+00:00  binance::DOGE_USDT      0.0897      0.0898      0.0895      0.0898  460666.00000      0.089629                23      0.089626 2022-05-31 14:38:29.472387+00:00
        """,
    "expected_length": 32,
    "expected_column_unique_values": {
        "full_symbol": ["binance::BTC_USDT", "binance::ADA_USDT"]
    },
    "expected_column_names": None
}
ccxt_config = config
ccxt_config["read_data"].update(expected)
test_read_data(ccxt_historical_client, **ccxt_config["read_data"])
