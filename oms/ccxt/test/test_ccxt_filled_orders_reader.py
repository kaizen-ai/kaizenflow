import datetime
import os
from typing import Any

import numpy as np
import pandas as pd

import helpers.hdbg as hdbg
import helpers.hio as hio
import helpers.hpandas as hpandas
import helpers.hunit_test as hunitest
import oms.ccxt.ccxt_filled_orders_reader as occforre


def _get_child_order_response_df() -> pd.DataFrame:
    """
    Generate synthetic data.
    """
    timestamps = [
        "2023-03-15 16:35:41.205000+00:00",
        "2023-03-15 16:35:42.205000+00:00",
        "2023-03-15 16:35:43.205000+00:00",
        "2023-03-15 16:35:44.205000+00:00",
    ]
    data = {
        "symbol": ["APE/USDT", "AVAX/USDT ", "APE/USDT", "AVAX/USDT "],
        "asset_id": [6051632686, 8717633868, 6051632686, 8717633868],
        "order": [7954906695, 4551306695, 7909906645, 3154906699],
        "client_order_id": [
            "x-xcKtGhcub89989e55d47273a3610a9",
            "x-xcKtGhcub03933b03766705d9d532f",
            "x-xcKtGhcue1c5f0b2aaddb6ba451d6c",
            "x-xcKtGhcub89981j55d47273a3610a9",
        ],
        "order_type": ["limit", "limit", "limit", "limit"],
        "time_in_force": ["GTC", "GTC", "GTC", "GTC"],
        "post_only": [False, False, False, False],
        "reduce_only": [False, False, False, False],
        "side": ["buy", "sell", "sell", "buy"],
        "order_price": [8.2, 9.85, 2.7, 10.1],
        "stop_price": [None, None, None, None],
        "order_amount": [10, 1, 0, 3],
        "order_update_timestamp": timestamps,
        "order_update_datetime": timestamps,
    }
    df = pd.DataFrame(data)
    df["order_update_datetime"] = pd.to_datetime(df["order_update_datetime"])
    return df


def _get_child_order_df() -> pd.DataFrame:
    """
    Generate synthetic data.
    """
    start_ts = [
        pd.Timestamp("2023-03-15 16:35:37.825835+0000", tz="UTC"),
        pd.Timestamp("2023-03-15 16:35:38.718960+0000", tz="UTC"),
        pd.Timestamp("2023-03-15 16:35:39.718960+0000", tz="UTC"),
        pd.Timestamp("2023-03-15 16:35:40.718960+0000", tz="UTC"),
    ]
    data = {
        "creation_timestamp": start_ts,
        "asset_id": [6051632686, 8717633868, 6051632686, 8717633868],
        "type_": ["limit", "limit", "limit", "limit"],
        "start_timestamp": start_ts,
        "end_timestamp": [
            pd.Timestamp("2023-03-15 16:36:37.825835+0000", tz="UTC"),
            pd.Timestamp("2023-03-15 16:36:38.718960+0000", tz="UTC"),
            pd.Timestamp("2023-03-15 16:36:39.825835+0000", tz="UTC"),
            pd.Timestamp("2023-03-15 16:36:40.718960+0000", tz="UTC"),
        ],
        "diff_num_shares": [10.0, -1.0, 10, -1],
        "limit_price": [4.12045, 15.804999, 12.88, 10.9],
    }
    df = pd.DataFrame(data)
    return df


def _get_fills_df() -> pd.DataFrame:
    """
    Generate synthetic data.
    """
    timestamps = [
        "2023-03-31 16:35:39.268000+00:00",
        "2023-03-31 16:35:40.268000+00:00",
        "2023-03-31 16:36:41.268000+00:00",
        "2023-03-31 16:36:42.268000+00:00",
    ]
    data = {
        "order": [7954906695, 4551306695, 7909906695, 3154906695],
        "id": [352825168, 762825168, 232825168, 972825168],
        "asset_id": ["6051632686", "8717633868", "6051632686", "8717633868"],
        "symbol": ["APE/USDT", "AVAX/USDT", "APE/USDT", "AVAX/USDT"],
        "buy_count": [1, 1, 0, 0],
        "sell_count": [0, 0, 1, 1],
        "taker_count": [1, 0, 1, 1],
        "maker_count": [0, 1, 0, 0],
        "price": [4.2, 5.85, 7.8, 3.9],
        "first_timestamp": timestamps,
        "last_timestamp": timestamps,
        "first_datetime": timestamps,
        "last_datetime": timestamps,
        "amount": [10, 1, 12, 5],
        "cost": [42.0, 5.85, 93.6, 19.5],
        "transaction_cost": [0.000009456, 0.0000096, 0.00000612, 0.0000056],
        "realized_pnl": [0, 0, 0, 0],
    }
    df = pd.DataFrame(data)
    datetime_col_names = [
        "first_timestamp",
        "last_timestamp",
        "first_datetime",
        "last_datetime",
    ]
    for col in datetime_col_names:
        df[col] = pd.to_datetime(df[col])
    return df


def _write_ccxt_trades(target_dir: str, version: str) -> None:
    """
    Generate and save JSON files with CCXT trades.
    """
    ccxt_trades_json1 = [
        {
            "info": {
                "symbol": "APEUSDT",
                "id": "352824923",
                "orderId": "7954906695",
                "side": "BUY",
                "price": "4.1200",
                "qty": "10",
                "realizedPnl": np.float64("0"),
                "marginAsset": "USDT",
                "quoteQty": "41.2000",
                "commission": "0.00824000",
                "commissionAsset": "USDT",
                "time": "1678898139268",
                "positionSide": "BOTH",
                "buyer": True,
                "maker": True,
            },
            "timestamp": 1678898139268,
            "datetime": pd.Timestamp("2023-03-15T16:35:39.268Z"),
            "symbol": "APE/USDT",
            "asset_id": 6051632686,
            "id": np.int64("352824923"),
            "order": np.int64("7954906695"),
            "type": None,
            "side": "buy",
            "takerOrMaker": "maker",
            "price": 4.12,
            "amount": 10.0,
            "cost": 41.2,
            "fee": {"cost": 0.00824, "currency": "USDT"},
            "fees": [{"currency": "USDT", "cost": 0.00824}],
        },
        {
            "info": {
                "symbol": "APEUSDT",
                "id": "352825168",
                "orderId": "7954913312",
                "side": "BUY",
                "price": "4.1100",
                "qty": "10",
                "realizedPnl": np.float64("0"),
                "marginAsset": "USDT",
                "quoteQty": "41.1000",
                "commission": "0.01644000",
                "commissionAsset": "USDT",
                "time": "1678898162247",
                "positionSide": "BOTH",
                "buyer": True,
                "maker": False,
            },
            "timestamp": 1678898162247,
            "datetime": pd.Timestamp("2023-03-15T16:36:02.247Z"),
            "symbol": "APE/USDT",
            "asset_id": 6051632686,
            "id": np.int64("352825168"),
            "order": np.int64("7954913312"),
            "type": None,
            "side": "buy",
            "takerOrMaker": "taker",
            "price": 4.11,
            "amount": 10.0,
            "cost": 41.1,
            "fee": {"cost": 0.01644, "currency": "USDT"},
            "fees": [{"currency": "USDT", "cost": 0.01644}],
        },
    ]
    ccxt_trades_json2 = [
        {
            "info": {
                "symbol": "AVAXUSDT",
                "id": "469887880",
                "orderId": "14412662588",
                "side": "BUY",
                "price": "15.5590",
                "qty": "2",
                "realizedPnl": np.float64("0"),
                "marginAsset": "USDT",
                "quoteQty": "31.1180",
                "commission": "0.01244720",
                "commissionAsset": "USDT",
                "time": "1678898523121",
                "positionSide": "BOTH",
                "buyer": True,
                "maker": False,
            },
            "timestamp": 1678898523121,
            "datetime": pd.Timestamp("2023-03-15T16:42:03.121Z"),
            "symbol": "AVAX/USDT",
            "asset_id": 8717633868,
            "id": np.int64("469887880"),
            "order": np.int64("14412662588"),
            "type": None,
            "side": "buy",
            "takerOrMaker": "taker",
            "price": 15.559,
            "amount": 2.0,
            "cost": 31.118,
            "fee": {"cost": 0.0124472, "currency": "USDT"},
            "fees": [{"currency": "USDT", "cost": 0.0124472}],
        },
        {
            "info": {
                "symbol": "AVAXUSDT",
                "id": "469890039",
                "orderId": "14412678428",
                "side": "BUY",
                "price": "15.5690",
                "qty": "2",
                "realizedPnl": np.float64("0"),
                "marginAsset": "USDT",
                "quoteQty": "31.1380",
                "commission": "0.01245520",
                "commissionAsset": "USDT",
                "time": "1678898583042",
                "positionSide": "BOTH",
                "buyer": True,
                "maker": False,
            },
            "timestamp": 1678898583042,
            "datetime": pd.Timestamp("2023-03-15T16:43:03.042Z"),
            "symbol": "AVAX/USDT",
            "asset_id": 8717633868,
            "id": np.int64("469890039"),
            "order": np.int64("14412678428"),
            "type": None,
            "side": "buy",
            "takerOrMaker": "taker",
            "price": 15.569,
            "amount": 2.0,
            "cost": 31.138,
            "fee": {"cost": 0.0124552, "currency": "USDT"},
            "fees": [{"currency": "USDT", "cost": 0.0124552}],
        },
    ]
    ccxt_trades_json3 = [
        {
            "info": {
                "symbol": "APEUSDT",
                "id": "352839121",
                "orderId": "7955072984",
                "side": "BUY",
                "price": "4.0380",
                "qty": "6",
                "realizedPnl": np.float64("0"),
                "marginAsset": "USDT",
                "quoteQty": "24.2280",
                "commission": "0.00484560",
                "commissionAsset": "USDT",
                "time": "1678898740515",
                "positionSide": "BOTH",
                "buyer": True,
                "maker": True,
            },
            "timestamp": 1678898740515,
            "datetime": pd.Timestamp("2023-03-15T16:45:40.515Z"),
            "symbol": "APE/USDT",
            "asset_id": 6051632686,
            "id": np.int64("352839121"),
            "order": np.int64("7955072984"),
            "type": None,
            "side": "buy",
            "takerOrMaker": "maker",
            "price": 4.038,
            "amount": 6.0,
            "cost": 24.228,
            "fee": {"cost": 0.0048456, "currency": "USDT"},
            "fees": [{"currency": "USDT", "cost": 0.0048456}],
        },
        {
            "info": {
                "symbol": "APEUSDT",
                "id": "352841649",
                "orderId": "7955107622",
                "side": "BUY",
                "price": "4.0560",
                "qty": "6",
                "realizedPnl": np.float64("0"),
                "marginAsset": "USDT",
                "quoteQty": "24.3360",
                "commission": "0.00973440",
                "commissionAsset": "USDT",
                "time": "1678898882325",
                "positionSide": "BOTH",
                "buyer": True,
                "maker": False,
            },
            "timestamp": 1678898882325,
            "datetime": pd.Timestamp("2023-03-15T16:48:02.325Z"),
            "symbol": "APE/USDT",
            "asset_id": 6051632686,
            "id": np.int64("352841649"),
            "order": np.int64("7955107622"),
            "type": None,
            "side": "buy",
            "takerOrMaker": "taker",
            "price": 4.056,
            "amount": 6.0,
            "cost": 24.336,
            "fee": {"cost": 0.0097344, "currency": "USDT"},
            "fees": [{"currency": "USDT", "cost": 0.0097344}],
        },
    ]
    ccxt_trades_json4 = [
        {
            "info": {
                "symbol": "APEUSDT",
                "id": "356819245",
                "orderId": "8077704766",
                "side": "SELL",
                "price": "4.0400",
                "qty": "6",
                "realizedPnl": np.float64("0"),
                "marginAsset": "USDT",
                "quoteQty": "24.2400",
                "commission": "0.00969600",
                "commissionAsset": "USDT",
                "time": "1679560540879",
                "positionSide": "BOTH",
                "buyer": False,
                "maker": False,
            },
            "timestamp": 1679560540879,
            "datetime": pd.Timestamp("2023-03-23T08:35:40.879Z"),
            "symbol": "APE/USDT",
            "asset_id": 6051632686,
            "id": np.int64("356819245"),
            "order": np.int64("8077704766"),
            "type": None,
            "side": "sell",
            "takerOrMaker": "taker",
            "price": 4.04,
            "amount": 6.0,
            "cost": 24.24,
            "fee": {"cost": 0.009696, "currency": "USDT"},
            "fees": [{"currency": "USDT", "cost": 0.009696}],
        },
        {
            "info": {
                "symbol": "AXSUSDT",
                "id": "420185708",
                "orderId": "8889302610",
                "side": "BUY",
                "price": "8.45500",
                "qty": "8",
                "realizedPnl": np.float64("0.11900000"),
                "marginAsset": "USDT",
                "quoteQty": "67.64000",
                "commission": "0.01352800",
                "commissionAsset": "USDT",
                "time": "1679560584427",
                "positionSide": "BOTH",
                "buyer": True,
                "maker": True,
            },
            "timestamp": 1679560584427,
            "datetime": pd.Timestamp("2023-03-23T08:36:24.427Z"),
            "symbol": "AXS/USDT",
            "asset_id": 2540896331,
            "id": np.int64("420185708"),
            "order": np.int64("8889302610"),
            "type": None,
            "side": "buy",
            "takerOrMaker": "maker",
            "price": 8.455,
            "amount": 8.0,
            "cost": 67.64,
            "fee": {"cost": 0.013528, "currency": "USDT"},
            "fees": [{"currency": "USDT", "cost": 0.013528}],
        },
    ]
    # Create JSON scratch directory based on the expected Broker version.
    if version == "v1":
        scratch_fills_path = os.path.join(
            target_dir,
            "child_order_fills",
        )
        file_name_prefix = ""
    elif version == "v2":
        scratch_fills_path = os.path.join(
            target_dir, "child_order_fills", "ccxt_trades"
        )
        file_name_prefix = "ccxt_trades_"
    else:
        raise ValueError(file_name_prefix)
    incremental = False
    hio.create_dir(scratch_fills_path, incremental)
    # Save JSON files.
    trades1_path = os.path.join(
        scratch_fills_path,
        f"{file_name_prefix}20230315_123830_20230315_123500.json",
    )
    hio.to_json(trades1_path, ccxt_trades_json1, use_types=True)
    trades2_path = os.path.join(
        scratch_fills_path,
        f"{file_name_prefix}20230315_124333_20230315_124000.json",
    )
    hio.to_json(trades2_path, ccxt_trades_json2, use_types=True)
    trades3_path = os.path.join(
        scratch_fills_path,
        f"{file_name_prefix}20230315_124930_20230315_124500.json",
    )
    hio.to_json(trades3_path, ccxt_trades_json3, use_types=True)
    trades4_path = os.path.join(
        scratch_fills_path,
        f"{file_name_prefix}20230315_163600_20230323_083700.json",
    )
    hio.to_json(trades4_path, ccxt_trades_json4, use_types=True)


def _write_ccxt_child_order_responses(target_dir: str) -> None:
    """
    Generate and save JSON files with CCXT child order responses.
    """
    order_resp1 = {
        "info": {
            "orderId": "7954906695",
            "symbol": "APEUSDT",
            "status": "NEW",
            "clientOrderId": "x-xcKtGhcub89989e55d47273a3610a9",
            "price": "4.1200",
            "avgPrice": "0.0000",
            "origQty": "10",
            "executedQty": "0",
            "cumQty": "0",
            "cumQuote": "0",
            "timeInForce": "GTC",
            "type": "LIMIT",
            "reduceOnly": False,
            "closePosition": False,
            "side": "BUY",
            "positionSide": "BOTH",
            "stopPrice": "0",
            "workingType": "CONTRACT_PRICE",
            "priceProtect": False,
            "origType": "LIMIT",
            "updateTime": np.int64("1678898138582"),
        },
        "id": np.int64("7954906695"),
        "clientOrderId": "x-xcKtGhcub89989e55d47273a3610a9",
        "timestamp": 1678898138582,
        "datetime": pd.Timestamp("2023-03-15T16:35:38.582Z"),
        "lastTradeTimestamp": None,
        "symbol": "APE/USDT",
        "type": "limit",
        "timeInForce": "GTC",
        "postOnly": False,
        "reduceOnly": False,
        "side": "buy",
        "price": np.float64(4.12),
        "stopPrice": np.float64(None),
        "amount": np.float64(10.0),
        "cost": np.float64(0.0),
        "average": np.float64(None),
        "filled": np.float64(0.0),
        "remaining": np.float64(10.0),
        "status": "open",
        "fee": np.float64(None),
        "trades": [],
        "fees": [],
    }
    order_resp2 = {
        "info": {
            "orderId": "14412582631",
            "symbol": "AVAXUSDT",
            "status": "NEW",
            "clientOrderId": "x-xcKtGhcub03933b03766705d9d532f",
            "price": "15.8050",
            "avgPrice": "0.0000",
            "origQty": "1",
            "executedQty": "0",
            "cumQty": "0",
            "cumQuote": "0",
            "timeInForce": "GTC",
            "type": "LIMIT",
            "reduceOnly": False,
            "closePosition": False,
            "side": "SELL",
            "positionSide": "BOTH",
            "stopPrice": "0",
            "workingType": "CONTRACT_PRICE",
            "priceProtect": False,
            "origType": "LIMIT",
            "updateTime": np.int64("1678898139409"),
        },
        "id": np.int64("14412582631"),
        "clientOrderId": "x-xcKtGhcub03933b03766705d9d532f",
        "timestamp": 1678898139409,
        "datetime": pd.Timestamp("2023-03-15T16:35:39.409Z"),
        "lastTradeTimestamp": None,
        "symbol": "AVAX/USDT",
        "type": "limit",
        "timeInForce": "GTC",
        "postOnly": False,
        "reduceOnly": False,
        "side": "sell",
        "price": np.float64(15.805),
        "stopPrice": np.float64(None),
        "amount": np.float64(1.0),
        "cost": np.float64(0.0),
        "average": np.float64(None),
        "filled": np.float64(0.0),
        "remaining": np.float64(1.0),
        "status": "open",
        "fee": np.float64(None),
        "trades": [],
        "fees": [],
    }
    # Create order responses scratch directory.
    scratch_order_resp_space = os.path.join(
        target_dir, "ccxt_child_order_responses"
    )
    incremental = False
    hio.create_dir(scratch_order_resp_space, incremental)
    # Save JSON files.
    order_resp1_path = os.path.join(
        scratch_order_resp_space,
        "20230315_123500.20230315_123538.json",
    )
    hio.to_json(order_resp1_path, order_resp1, use_types=True)
    order_resp2_path = os.path.join(
        scratch_order_resp_space,
        "20230315_123500.20230315_123539.json",
    )
    hio.to_json(order_resp2_path, order_resp2, use_types=True)


def _write_oms_child_orders(target_dir: str, version: str) -> None:
    """
    Generate and save individual OMS child orders as CSV files.
    """
    child_order1 = {
        "order_id": 20,
        "creation_timestamp": pd.Timestamp("2023-03-15 12:35:37.825835-04:00"),
        "asset_id": np.int64(6051632686),
        "type_": "limit",
        "start_timestamp": pd.Timestamp("2023-03-15 12:35:37.825835-04:00"),
        "end_timestamp": pd.Timestamp("2023-03-15 12:36:37.825835-04:00"),
        "curr_num_shares": np.float64(0.0),
        "diff_num_shares": np.float64(10.0),
        "tz": "America/New_York",
        "passivity_factor": np.float64(0.55),
        "latest_bid_price": np.float64(4.12),
        "latest_ask_price": np.float64(4.121),
        "bid_price_mean": np.float64(4.125947368421053),
        "ask_price_mean": np.float64(4.126973684210526),
        "used_bid_price": "latest_bid_price",
        "used_ask_price": "latest_ask_price",
        "limit_price": np.float64(4.12045),
        "ccxt_id": np.int64(7954906695),
    }
    child_order2 = {
        "order_id": 21,
        "creation_timestamp": pd.Timestamp("2023-03-15 12:35:38.718960-04:00"),
        "asset_id": np.int64(8717633868),
        "type_": "limit",
        "start_timestamp": pd.Timestamp("2023-03-15 12:35:38.718960-04:00"),
        "end_timestamp": pd.Timestamp("2023-03-15 12:36:38.718960-04:00"),
        "curr_num_shares": np.float64(0.0),
        "diff_num_shares": np.float64(-1.0),
        "tz": "America/New_York",
        "passivity_factor": np.float64(0.55),
        "latest_bid_price": np.float64(15.804),
        "latest_ask_price": np.float64(15.805),
        "bid_price_mean": np.float64(15.81816216216216),
        "ask_price_mean": np.float64(15.819432432432428),
        "used_bid_price": "latest_bid_price",
        "used_ask_price": "latest_ask_price",
        "limit_price": np.float64(15.804549999999999),
        "ccxt_id": np.int64(14412582631),
    }
    # Generate subdir name and adde extra params based on Broker version.
    if version == "v1":
        oms_orders_scratch_space = os.path.join(target_dir, "oms_child_orders")
        # Add timestamp logs to extra parameters corresponding to logging from CcxtBroker_v1.
        child_order1_extra_params = {
            "submit_single_order_to_ccxt.start.timestamp": datetime.datetime(
                2023, 5, 23, 11, 58, 51, 97108
            ),
            "submit_single_order_to_ccxt.end.timestamp": datetime.datetime(
                2023, 5, 23, 11, 58, 51, 97500
            ),
            "submit_single_order_to_ccxt.num_attempts": 20,
        }
        child_order1["extra_params"] = child_order1_extra_params

        child_order2_extra_params = {
            "submit_single_order_to_ccxt.start.timestamp": datetime.datetime(
                2023, 5, 23, 11, 58, 50, 584038
            ),
            "submit_single_order_to_ccxt.end.timestamp": datetime.datetime(
                2023, 5, 23, 11, 58, 50, 584405
            ),
            "submit_single_order_to_ccxt.num_attempts": 21,
        }
        child_order2["extra_params"] = child_order2_extra_params
    elif version == "v2":
        oms_orders_scratch_space = os.path.join(target_dir, "oms_child_orders")
        # Add timestamp logs to extra parameters corresponding to logging from CcxtBroker_v2.
        child_order1_extra_params = {
            "order_generated_timestamp": datetime.datetime(
                2023, 5, 23, 11, 58, 51, 97108
            ),
            "order_prepared_for_submission_inside_class_timestamp": datetime.datetime(
                2023, 5, 23, 11, 58, 51, 97500
            ),
            "order_submitted_to_ccxt_inside_class_timestamp": datetime.datetime(
                2023, 5, 23, 11, 58, 51, 363107
            ),
            "ccxt_id": 11381353660,
            "order_submitted_inside_coroutine_timestamp": datetime.datetime(
                2023, 5, 23, 11, 58, 51, 363716
            ),
            "after_sleep_inside_coroutine_timestamp": datetime.datetime(
                2023, 5, 23, 11, 59, 1, 299605
            ),
            "cancelled_open_order_timestamp": datetime.datetime(
                2023, 5, 23, 11, 59, 1, 548572
            ),
            "order_coroutine_timed_scope_output": "generating_single_order_and_submitting done (10.455 s)",
        }
        child_order1["extra_params"] = child_order1_extra_params
        child_order2_extra_params = {
            "order_generated_timestamp": datetime.datetime(
                2023, 5, 23, 11, 58, 50, 584038
            ),
            "order_prepared_for_submission_inside_class_timestamp": datetime.datetime(
                2023, 5, 23, 11, 58, 50, 584405
            ),
            "order_submitted_to_ccxt_inside_class_timestamp": datetime.datetime(
                2023, 5, 23, 11, 58, 50, 835601
            ),
            "ccxt_id": 8389765599793650580,
            "order_submitted_inside_coroutine_timestamp": datetime.datetime(
                2023, 5, 23, 11, 58, 50, 836047
            ),
            "after_sleep_inside_coroutine_timestamp": datetime.datetime(
                2023, 5, 23, 11, 59, 3, 800553
            ),
            "cancelled_open_order_timestamp": datetime.datetime(
                2023, 5, 23, 11, 59, 4, 49388
            ),
            "order_coroutine_timed_scope_output": "generating_single_order_and_submitting done (13.469 s)",
        }
        child_order2["extra_params"] = child_order2_extra_params
    # Create child orders scratch directory.
    incremental = False
    hio.create_dir(oms_orders_scratch_space, incremental)
    # Save JSON files.
    child_order1_path = os.path.join(
        oms_orders_scratch_space,
        "20230315_123500.20230315_123538.json",
    )
    hio.to_json(child_order1_path, child_order1, use_types=True)
    child_order2_path = os.path.join(
        oms_orders_scratch_space,
        "20230315_123500.20230315_123539.json",
    )
    hio.to_json(child_order2_path, child_order2, use_types=True)


def _write_oms_parent_orders(target_dir: str) -> None:
    """
    Generate and save JSON file with OMS parent orders.
    """
    parent_order1 = {
        "order_id": 0,
        "creation_timestamp": pd.Timestamp("2023-03-15 12:34:38.718960-04:00"),
        "asset_id": np.int64(6051632686),
        "type_": "limit",
        "start_timestamp": pd.Timestamp("2023-03-15 12:34:38.718960-04:00"),
        "end_timestamp": pd.Timestamp("2023-03-15 12:39:38.718960-04:00"),
        "curr_num_shares": np.float64(0.0),
        "diff_num_shares": np.float64(50.0),
        "tz": "America/New_York",
        "extra_params": dict(),
    }
    parent_order2 = {
        "order_id": 1,
        "creation_timestamp": pd.Timestamp("2023-03-15 12:34:39.718960-04:00"),
        "asset_id": np.int64(8717633868),
        "type_": "limit",
        "start_timestamp": pd.Timestamp("2023-03-15 12:34:39.718960-04:00"),
        "end_timestamp": pd.Timestamp("2023-03-15 12:39:39.718960-04:00"),
        "curr_num_shares": np.float64(0.0),
        "diff_num_shares": np.float64(-5.0),
        "tz": "America/New_York",
        "extra_params": dict(),
    }
    parent_orders = [parent_order1, parent_order2]
    # Create OMS parent orders child directory.
    oms_parent_orders_scratch_space = os.path.join(
        target_dir, "oms_parent_orders"
    )
    incremental = False
    hio.create_dir(oms_parent_orders_scratch_space, incremental)
    # Save JSON file.
    parent_orders_path = os.path.join(
        oms_parent_orders_scratch_space,
        "oms_parent_orders_20230315_123500.20230315_123539.json",
    )
    hio.to_json(parent_orders_path, parent_orders, use_types=True)


def _write_ccxt_fills(target_dir: str) -> None:
    """
    Generate and save JSON files with CCXT fills.
    """
    ccxt_fills1 = [
        {
            "info": {
                "orderId": "14901643572",
                "symbol": "AVAXUSDT",
                "status": "FILLED",
                "clientOrderId": "x-xcKtGhcu8261da0e4a2533b528e0e2",
                "price": 14.7610,
                "avgPrice": 14.7540,
                "origQty": 1,
                "executedQty": "1",
                "cumQuote": "14.7540",
                "timeInForce": "GTC",
                "type": "LIMIT",
                "reduceOnly": False,
                "closePosition": False,
                "side": "BUY",
                "positionSide": "BOTH",
                "stopPrice": "0",
                "workingType": "CONTRACT_PRICE",
                "priceProtect": False,
                "origType": "LIMIT",
                "time": "1684843129172",
                "updateTime": np.int64("1684843129172"),
            },
            "id": np.int64("14901643572"),
            "clientOrderId": "x-xcKtGhcu8261da0e4a2533b528e0e2",
            "timestamp": 1684843129172,
            "datetime": pd.Timestamp("2023-05-23T11:58:49.172Z"),
            "lastTradeTimestamp": None,
            "symbol": "AVAX/USDT",
            "type": "limit",
            "timeInForce": "GTC",
            "postOnly": False,
            "reduceOnly": False,
            "side": "buy",
            "price": np.float64(14.761),
            "stopPrice": np.float64(None),
            "amount": np.float64(1.0),
            "cost": np.float64(14.754),
            "average": np.float64(14.754),
            "filled": np.float64(1.0),
            "remaining": np.float64(0.0),
            "status": "closed",
            "fee": np.float64(None),
            "trades": [],
            "fees": [],
        },
        {
            "info": {
                "orderId": "153017797793",
                "symbol": "BTCUSDT",
                "status": "FILLED",
                "clientOrderId": "x-xcKtGhcua8e7e261598721e6c6295c",
                "price": "27341.90",
                "avgPrice": "27330.40000",
                "origQty": "0.001",
                "executedQty": "0.001",
                "cumQuote": "27.33040",
                "timeInForce": "GTC",
                "type": "LIMIT",
                "reduceOnly": False,
                "closePosition": False,
                "side": "BUY",
                "positionSide": "BOTH",
                "stopPrice": "0",
                "workingType": "CONTRACT_PRICE",
                "priceProtect": False,
                "origType": "LIMIT",
                "time": "1684843129687",
                "updateTime": np.int64("1684843129687"),
            },
            "id": np.int64("153017797793"),
            "clientOrderId": "x-xcKtGhcua8e7e261598721e6c6295c",
            "timestamp": 1684843129687,
            "datetime": pd.Timestamp("2023-05-23T11:58:49.687Z"),
            "lastTradeTimestamp": None,
            "symbol": "BTC/USDT",
            "type": "limit",
            "timeInForce": "GTC",
            "postOnly": False,
            "reduceOnly": False,
            "side": "buy",
            "price": np.float64(27341.9),
            "stopPrice": np.float64(None),
            "amount": np.float64(0.001),
            "cost": np.float64(27.3304),
            "average": np.float64(27330.4),
            "filled": np.float64(0.001),
            "remaining": np.float64(0.0),
            "status": "closed",
            "fee": np.float64(None),
            "trades": [],
            "fees": [],
        },
    ]
    ccxt_fills2 = [
        {
            "info": {
                "orderId": "2187830797",
                "symbol": "CTKUSDT",
                "status": "FILLED",
                "clientOrderId": "x-xcKtGhcuda5dde8563a5c1568a5893",
                "price": "0.74810",
                "avgPrice": "0.74810",
                "origQty": "93",
                "executedQty": "93",
                "cumQuote": "69.57330",
                "timeInForce": "GTC",
                "type": "LIMIT",
                "reduceOnly": False,
                "closePosition": False,
                "side": "BUY",
                "positionSide": "BOTH",
                "stopPrice": "0",
                "workingType": "CONTRACT_PRICE",
                "priceProtect": False,
                "origType": "LIMIT",
                "time": "1684843130201",
                "updateTime": np.int64("1684843130201"),
            },
            "id": np.int64("2187830797"),
            "clientOrderId": "x-xcKtGhcuda5dde8563a5c1568a5893",
            "timestamp": 1684843130201,
            "datetime": pd.Timestamp("2023-05-23T11:58:50.201Z"),
            "lastTradeTimestamp": None,
            "symbol": "CTK/USDT",
            "type": "limit",
            "timeInForce": "GTC",
            "postOnly": False,
            "reduceOnly": False,
            "side": "buy",
            "price": np.float64(0.7481),
            "stopPrice": np.float64(None),
            "amount": np.float64(93.0),
            "cost": np.float64(69.5733),
            "average": np.float64(0.7481),
            "filled": np.float64(93.0),
            "remaining": np.float64(0.0),
            "status": "closed",
            "fee": np.float64(None),
            "trades": [],
            "fees": [],
        },
        {
            "info": {
                "orderId": "8378993779",
                "symbol": "DYDXUSDT",
                "status": "FILLED",
                "clientOrderId": "x-xcKtGhcucc938a1e2341d522e5f1da",
                "price": "2.180",
                "avgPrice": "2.1820",
                "origQty": "3",
                "executedQty": "3",
                "cumQuote": "6.5460",
                "timeInForce": "GTC",
                "type": "LIMIT",
                "reduceOnly": False,
                "closePosition": False,
                "side": "SELL",
                "positionSide": "BOTH",
                "stopPrice": "0",
                "workingType": "CONTRACT_PRICE",
                "priceProtect": False,
                "origType": "LIMIT",
                "time": "1684843130456",
                "updateTime": np.int64("1684843130456"),
            },
            "id": np.int64("8378993779"),
            "clientOrderId": "x-xcKtGhcucc938a1e2341d522e5f1da",
            "timestamp": 1684843130456,
            "datetime": pd.Timestamp("2023-05-23T11:58:50.456Z"),
            "lastTradeTimestamp": None,
            "symbol": "DYDX/USDT",
            "type": "limit",
            "timeInForce": "GTC",
            "postOnly": False,
            "reduceOnly": False,
            "side": "sell",
            "price": np.float64(2.18),
            "stopPrice": np.float64(False),
            "amount": np.float64(3.0),
            "cost": np.float64(6.546),
            "average": np.float64(2.182),
            "filled": np.float64(3.0),
            "remaining": np.float64(0.0),
            "status": "closed",
            "fee": np.float64(False),
            "trades": [],
            "fees": [],
        },
    ]
    ccxt_fills_scratch_space = os.path.join(
        target_dir, "child_order_fills", "ccxt_fills"
    )
    # Create child orders scratch directory.
    incremental = False
    hio.create_dir(ccxt_fills_scratch_space, incremental)
    # Save JSON files.
    ccxt_fills1_path = os.path.join(
        ccxt_fills_scratch_space,
        "ccxt_fills_20230315_123500.20230315_123538.json",
    )
    hio.to_json(ccxt_fills1_path, ccxt_fills1, use_types=True)
    ccxt_fills2_path = os.path.join(
        ccxt_fills_scratch_space,
        "ccxt_fills_20230315_123500.20230315_123539.json",
    )
    hio.to_json(ccxt_fills2_path, ccxt_fills2, use_types=True)


def _write_oms_fills(target_dir: str) -> None:
    """
    Generate and save JSON files with OMS fills.
    """
    oms_fills1 = [
        {
            "asset_id": np.int64(8717633868),
            "fill_id": np.int64(2),
            "timestamp": pd.Timestamp("2023-05-23T11:58:49.172000+00:00"),
            "num_shares": np.float64(1.0),
            "price": np.float64(14.7540),
        },
        {
            "asset_id": np.int64(1467591036),
            "fill_id": np.int64(4),
            "timestamp": pd.Timestamp("2023-05-23T11:58:49.687000+00:00"),
            "num_shares": np.float64(0.001),
            "price": np.float64(27.3304),
        },
    ]
    oms_fills2 = [
        {
            "asset_id": np.int64(5115052901),
            "fill_id": np.int64(6),
            "timestamp": pd.Timestamp("2023-05-23T11:58:50.201000+00:00"),
            "num_shares": np.float64(93.0),
            "price": np.float64(69.5733),
        },
        {
            "asset_id": np.int64(3401245610),
            "fill_id": np.int64(7),
            "timestamp": pd.Timestamp("2023-05-23T11:58:50.456000+00:00"),
            "num_shares": np.float64(3.0),
            "price": np.float64(6.546),
        },
    ]
    oms_fills_scratch_space = os.path.join(
        target_dir, "child_order_fills", "oms_fills"
    )
    # Create child orders scratch directory.
    incremental = False
    hio.create_dir(oms_fills_scratch_space, incremental)
    # Save CSV files.
    oms_fills1_path = os.path.join(
        oms_fills_scratch_space,
        "oms_fills_20230315_123500.20230315_123538.json",
    )
    hio.to_json(oms_fills1_path, oms_fills1, use_types=True)
    oms_fills2_path = os.path.join(
        oms_fills_scratch_space,
        "oms_fills_20230315_123500.20230315_123539.json",
    )
    hio.to_json(oms_fills2_path, oms_fills2, use_types=True)


def _write_test_data(target_dir: str, version: str) -> None:
    """
    Write test data into the scratch directory.

    :param version: which version of the Broker should fit the data:
    - 'v1' for logs generated by `CcxtBroker`,
    - 'v2' for logs generated by `CcxtBroker_v2`
    """
    hdbg.dassert_in(version, ["v1", "v2"])
    # Generate and save CCXT trades.
    _write_ccxt_trades(target_dir, version)
    # Generate and save CCXT order responses.
    _write_ccxt_child_order_responses(target_dir)
    # Generate and save OMS child orders.
    _write_oms_child_orders(target_dir, version)
    # Generate and save OMS parent orders.
    _write_oms_parent_orders(target_dir)
    if version == "v2":
        # Generate and save CCXT fills.
        _write_ccxt_fills(target_dir)
        # Generate and save OMS fills.
        _write_oms_fills(target_dir)


def _check(self_: Any, actual: pd.DataFrame, expected: str) -> None:
    actual = hpandas.df_to_str(actual)
    self_.assert_equal(actual, expected, fuzzy_match=True)


class TestCcxtLogsReader1(hunitest.TestCase):
    def test_init1(self) -> None:
        """
        Verify that the CCXT logs reader v1 is initialized correctly.
        """
        target_dir = self.get_scratch_space()
        broker_version = "v1"
        _write_test_data(target_dir, broker_version)
        reader = occforre.CcxtLogsReader(target_dir)
        expected_fills_dir = os.path.join(target_dir, "child_order_fills")
        self.assertEqual(reader._ccxt_trades_dir, expected_fills_dir)
        expected_child_orders_dir = os.path.join(target_dir, "oms_child_orders")
        self.assertEqual(reader._oms_child_orders_dir, expected_child_orders_dir)
        expected_order_responses_dir = os.path.join(
            target_dir, "ccxt_child_order_responses"
        )
        self.assertEqual(
            reader._ccxt_order_responses_dir, expected_order_responses_dir
        )

    def test_init2(self) -> None:
        """
        Verify that the CCXT logs for Broker v2 is initialized correctly.
        """
        target_dir = self.get_scratch_space()
        broker_version = "v2"
        _write_test_data(target_dir, broker_version)
        reader = occforre.CcxtLogsReader(target_dir)
        #
        expected_trades_dir = os.path.join(
            target_dir, "child_order_fills", "ccxt_trades"
        )
        self.assertEqual(reader._ccxt_trades_dir, expected_trades_dir)
        #
        expected_child_orders_dir = os.path.join(target_dir, "oms_child_orders")
        self.assertEqual(reader._oms_child_orders_dir, expected_child_orders_dir)
        #
        expected_oms_parent_orders_dir = os.path.join(
            target_dir, "oms_parent_orders"
        )
        self.assertEqual(
            reader._oms_parent_orders_dir, expected_oms_parent_orders_dir
        )
        #
        expected_order_responses_dir = os.path.join(
            target_dir, "ccxt_child_order_responses"
        )
        self.assertEqual(
            reader._ccxt_order_responses_dir, expected_order_responses_dir
        )
        #
        expected_ccxt_fills_dir = os.path.join(
            target_dir, "child_order_fills", "ccxt_fills"
        )
        self.assertEqual(reader._ccxt_fills_dir, expected_ccxt_fills_dir)
        #
        expected_oms_fills_dir = os.path.join(
            target_dir, "child_order_fills", "oms_fills"
        )
        self.assertEqual(reader._oms_fills_dir, expected_oms_fills_dir)

    def test_get_files1(self) -> None:
        """
        Verify that files in the scratch space for v1 are listed correctly.
        """
        # Create fills data.
        target_dir = self.get_scratch_space()
        broker_version = "v1"
        _write_test_data(target_dir, broker_version)
        reader = occforre.CcxtLogsReader(target_dir)
        #
        actual_trades_files = reader._get_files(reader._ccxt_trades_dir)
        expected_trades_files = [
            os.path.join(
                target_dir,
                "child_order_fills",
                "20230315_123830_20230315_123500.json",
            ),
            os.path.join(
                target_dir,
                "child_order_fills",
                "20230315_124333_20230315_124000.json",
            ),
            os.path.join(
                target_dir,
                "child_order_fills",
                "20230315_124930_20230315_124500.json",
            ),
            os.path.join(
                target_dir,
                "child_order_fills",
                "20230315_163600_20230323_083700.json",
            ),
        ]
        self.assertListEqual(actual_trades_files, expected_trades_files)

    def test_get_files2(self) -> None:
        """
        Verify that files in the scratch space for Broker v2 are listed
        correctly.
        """
        # Create fills data.
        target_dir = self.get_scratch_space()
        broker_version = "v2"
        _write_test_data(target_dir, broker_version)
        reader = occforre.CcxtLogsReader(target_dir)
        #
        actual_trades_files = reader._get_files(reader._ccxt_trades_dir)
        expected_trades_files = [
            os.path.join(
                target_dir,
                "child_order_fills",
                "ccxt_trades",
                "ccxt_trades_20230315_123830_20230315_123500.json",
            ),
            os.path.join(
                target_dir,
                "child_order_fills",
                "ccxt_trades",
                "ccxt_trades_20230315_124333_20230315_124000.json",
            ),
            os.path.join(
                target_dir,
                "child_order_fills",
                "ccxt_trades",
                "ccxt_trades_20230315_124930_20230315_124500.json",
            ),
            os.path.join(
                target_dir,
                "child_order_fills",
                "ccxt_trades",
                "ccxt_trades_20230315_163600_20230323_083700.json",
            ),
        ]
        self.assertListEqual(actual_trades_files, expected_trades_files)

    def test_load_oms_child_order_df1(self) -> None:
        """
        Verify that child orders are loaded and transformed correctly without
        extra parameters unpacked.

        Loads child orders for v1 Broker.
        """
        # Create fake data.
        target_dir = self.get_scratch_space()
        broker_version = "v1"
        _write_test_data(target_dir, broker_version)
        # Read fake data.
        reader = occforre.CcxtLogsReader(target_dir)
        child_orders = reader.load_oms_child_order_df(unpack_extra_params=False)
        # Check.
        actual = hpandas.df_to_str(child_orders)
        self.check_string(actual)

    def test_load_oms_child_order_df2(self) -> None:
        """
        Verify that child orders are loaded and transformed correctly without
        extra parameters unpacked.

        Loads child orders for v2 Broker.
        """
        target_dir = self.get_scratch_space()
        broker_version = "v2"
        _write_test_data(target_dir, broker_version)
        reader = occforre.CcxtLogsReader(target_dir)
        #
        child_orders = reader.load_oms_child_order_df(unpack_extra_params=False)
        actual = hpandas.df_to_str(child_orders)
        #
        self.check_string(actual)

    def test_load_oms_child_order_df3(self) -> None:
        """
        Verify that child orders are loaded and transformed correctly and with
        extra parameters unpacked.

        Loads child orders for v2 Broker.
        """
        target_dir = self.get_scratch_space()
        broker_version = "v2"
        _write_test_data(target_dir, broker_version)
        reader = occforre.CcxtLogsReader(target_dir)
        #
        child_orders = reader.load_oms_child_order_df(unpack_extra_params=True)
        actual = hpandas.df_to_str(child_orders)
        #
        self.check_string(actual)

    def test_load_oms_parent_order_df(self) -> None:
        """
        Verify that parent orders are loaded and normalized correctly.
        """
        target_dir = self.get_scratch_space()
        broker_version = "v2"
        _write_test_data(target_dir, broker_version)
        reader = occforre.CcxtLogsReader(target_dir)
        #
        parent_orders = reader.load_oms_parent_order_df()
        actual = hpandas.df_to_str(parent_orders)
        #
        self.check_string(actual)

    def test_load_ccxt_trades_df(self) -> None:
        """
        Verify that ccxt trades are loaded and transformed correctly.
        """
        target_dir = self.get_scratch_space()
        broker_version = "v1"
        _write_test_data(target_dir, broker_version)
        reader = occforre.CcxtLogsReader(target_dir)
        #
        child_order_fills = reader.load_ccxt_trades_df()
        actual = hpandas.df_to_str(child_order_fills)
        self.check_string(actual)

    def test_load_ccxt_order_response_df(self) -> None:
        """
        Verify that child order responses from CCXT are loaded and transformed
        correctly.
        """
        target_dir = self.get_scratch_space()
        broker_version = "v1"
        _write_test_data(target_dir, broker_version)
        reader = occforre.CcxtLogsReader(target_dir)
        #
        ccxt_child_order_responses = reader.load_ccxt_order_response_df()
        actual = hpandas.df_to_str(ccxt_child_order_responses)
        self.check_string(actual)

    def test_load_ccxt_fills_df(self) -> None:
        """
        Verify that CCXT fills are loaded and transformed correctly.
        """
        target_dir = self.get_scratch_space()
        broker_version = "v2"
        _write_test_data(target_dir, broker_version)
        reader = occforre.CcxtLogsReader(target_dir)
        ccxt_fills_df = reader.load_ccxt_fills_df()
        actual = hpandas.df_to_str(ccxt_fills_df)
        self.check_string(actual)

    def test_load_oms_fills_df(self) -> None:
        """
        Verify that OMS fills are loaded and transformed correctly.
        """
        target_dir = self.get_scratch_space()
        broker_version = "v2"
        _write_test_data(target_dir, broker_version)
        reader = occforre.CcxtLogsReader(target_dir)
        #
        oms_fills_df = reader.load_oms_fills_df()
        actual = hpandas.df_to_str(oms_fills_df)
        self.check_string(actual)

    def test_load_all_data(self) -> None:
        """
        Verify that all data is loaded correctly.
        """
        target_dir = self.get_scratch_space()
        broker_version = "v2"
        _write_test_data(target_dir, broker_version)
        reader = occforre.CcxtLogsReader(target_dir)
        #
        all_data = reader.load_all_data()
        # Assert the method loads the expected data types.
        self.assertEqual(
            list(all_data),
            [
                "ccxt_order_responses",
                "oms_parent_orders",
                "ccxt_trades",
                "oms_child_orders",
                "oms_fills",
                "ccxt_fills",
            ],
        )
        # Assert the dataframes are not empty.
        for df in all_data.values():
            self.assertFalse(df.empty)


class Test_read_rt_data1(hunitest.TestCase):
    """
    Read back data from the logs of a real time execution done with
    run_ccxt_broker_v2.py.
    """

    def test_load_ccxt_order_response_df(self) -> None:
        """
        Verify that child order responses from CCXT are loaded and transformed
        correctly.
        """
        reader = self._get_reader()
        ccxt_order_response = reader.load_ccxt_order_response_df()
        actual = hpandas.df_to_str(ccxt_order_response)
        self.check_string(actual)

    def test_load_ccxt_trades_df(self) -> None:
        """
        Verify that ccxt trades are loaded and transformed correctly.
        """
        reader = self._get_reader()
        daccxt_trades = reader.load_ccxt_trades_df()
        actual = hpandas.df_to_str(daccxt_trades)
        self.check_string(actual)

    def test_load_oms_child_order_df(self) -> None:
        """
        Verify that child orders are loaded and transformed correctly without
        extra parameters unpacked.

        Loads child orders for v2 Broker.
        """
        reader = self._get_reader()
        oms_child_order = reader.load_oms_child_order_df(
            unpack_extra_params=False
        )
        actual = hpandas.df_to_str(oms_child_order)
        self.check_string(actual)

    def test_load_oms_child_order_df1(self) -> None:
        """
        Verify that child orders are loaded and transformed correctly and with
        extra parameters unpacked.

        Loads child orders for v2 Broker.
        """
        reader = self._get_reader()
        oms_child_order = reader.load_oms_child_order_df(unpack_extra_params=True)
        actual = hpandas.df_to_str(oms_child_order)
        self.check_string(actual)

    def test_load_oms_parent_order_df1(self) -> None:
        """
        Verify that parent orders are loaded and transformed correctly.
        """
        reader = self._get_reader()
        oms_parent_orders = reader.load_oms_parent_order_df()
        actual = hpandas.df_to_str(oms_parent_orders)
        self.check_string(actual)

    def _get_reader(
        self,
    ) -> occforre.CcxtLogsReader:
        """
        Get `CcxtLogsReader` instance.
        """
        target_dir = os.path.join(self._base_dir_name, "rt_data")
        reader = occforre.CcxtLogsReader(target_dir)
        return reader


class Test_aggregate_fills_by_bar(hunitest.TestCase):
    """
    Test that a fills dataframe aggregates by order id, then by bar at `freq`
    correctly.
    """

    def test1(self) -> None:
        fills_df = _get_fills_df()
        freq = "T"
        actual = occforre.aggregate_fills_by_bar(fills_df, freq)
        expected = r"""
                                                            first_timestamp                   last_timestamp                   first_datetime                    last_datetime     symbol    asset_id  buy_count  sell_count  taker_count  maker_count  price  amount   cost  transaction_cost  realized_pnl
        bar_end_datetime          asset_id
        2023-03-31 16:36:00+00:00 6051632686 2023-03-31 16:35:39.268000+00:00 2023-03-31 16:35:39.268000+00:00 2023-03-31 16:35:39.268000+00:00 2023-03-31 16:35:39.268000+00:00   APE/USDT  6051632686          1           0            1            0   4.20      10  42.00          0.000009             0
                                  8717633868 2023-03-31 16:35:40.268000+00:00 2023-03-31 16:35:40.268000+00:00 2023-03-31 16:35:40.268000+00:00 2023-03-31 16:35:40.268000+00:00  AVAX/USDT  8717633868          1           0            0            1   5.85       1   5.85          0.000010             0
        2023-03-31 16:37:00+00:00 6051632686 2023-03-31 16:36:41.268000+00:00 2023-03-31 16:36:41.268000+00:00 2023-03-31 16:36:41.268000+00:00 2023-03-31 16:36:41.268000+00:00   APE/USDT  6051632686          0           1            1            0   7.80      12  93.60          0.000006             0
                                  8717633868 2023-03-31 16:36:42.268000+00:00 2023-03-31 16:36:42.268000+00:00 2023-03-31 16:36:42.268000+00:00 2023-03-31 16:36:42.268000+00:00  AVAX/USDT  8717633868          0           1            1            0   3.90       5  19.50          0.000006             0
        """
        _check(self, actual, expected)


class Test_aggregate_fills_by_order(hunitest.TestCase):
    """
    Test that fills dataframe aggregates by order id correctly.
    """

    def test1(self) -> None:
        fills_df = _get_fills_df()
        fills_df = fills_df.loc[fills_df["symbol"] == "APE/USDT"]
        #
        actual = occforre.aggregate_fills_by_order(fills_df)
        expected = r"""
                                    first_timestamp                   last_timestamp                   first_datetime                    last_datetime    symbol    asset_id  buy_count  sell_count  taker_count  maker_count  price  amount  cost  transaction_cost  realized_pnl
        order
        7909906695 2023-03-31 16:36:41.268000+00:00 2023-03-31 16:36:41.268000+00:00 2023-03-31 16:36:41.268000+00:00 2023-03-31 16:36:41.268000+00:00  APE/USDT  6051632686          0           1            1            0    7.8      12  93.6          0.000006             0
        7954906695 2023-03-31 16:35:39.268000+00:00 2023-03-31 16:35:39.268000+00:00 2023-03-31 16:35:39.268000+00:00 2023-03-31 16:35:39.268000+00:00  APE/USDT  6051632686          1           0            1            0    4.2      10  42.0          0.000009             0
        """
        _check(self, actual, expected)


class Test_compute_buy_sell_prices_by_bar(hunitest.TestCase):
    """
    Check that buy/sell trade prices by symbol and bar are computed correctly.
    """

    def test1(self) -> None:
        fills_df = _get_fills_df()
        freq = "T"
        actual = occforre.compute_buy_sell_prices_by_bar(fills_df, freq)
        # There is no buy / sell order at timestamps with NaN values in the row.
        expected = r"""
                                buy_trade_price            sell_trade_price
        asset_id                       6051632686 8717633868       6051632686 8717633868
        bar_end_datetime
        2023-03-31 16:36:00+00:00             4.2       5.85              NaN        NaN
        2023-03-31 16:37:00+00:00             NaN        NaN              7.8        3.9
        """
        _check(self, actual, expected)


class Test_get_limit_order_price(hunitest.TestCase):
    """
    Verify that `get_limit_order_price()` gets limit order prices from orders
    correctly.
    """

    def test1(self) -> None:
        df = _get_child_order_df()
        freq = "T"
        actual = occforre.get_limit_order_price(df, freq=freq)
        expected = r"""
                                buy_limit_order_price            sell_limit_order_price
        asset_id                             6051632686 8717633868             6051632686 8717633868
        creation_timestamp
        2023-03-15 16:36:00+00:00              8.500225        NaN                    NaN    13.3525
        """
        _check(self, actual, expected)


class Test_aggregate_ccxt_orders_by_bar(hunitest.TestCase):
    """
    Verify that orders are aggregated by bar correctly.
    """

    def test1(self) -> None:
        child_order_response_df = _get_child_order_response_df()
        freq = "T"
        actual = occforre.aggregate_ccxt_orders_by_bar(
            child_order_response_df, freq
        )
        expected = r"""
                                order_count            buy_order_count            buy_limit_twap            buy_limit_vwap            buy_amount            buy_notional            sell_order_count            sell_limit_twap            sell_limit_vwap            sell_amount            sell_notional
        asset_id                   2766533309 6051632686      2766533309 6051632686     2766533309 6051632686     2766533309 6051632686 2766533309 6051632686   2766533309 6051632686       2766533309 6051632686      2766533309 6051632686      2766533309 6051632686  2766533309 6051632686    2766533309 6051632686
        bar_end_datetime
        2023-03-15 16:36:00+00:00           2          2               1          1           10.1        8.2           10.1        8.2          3         10         30.3       82.0                1          1            9.85        2.7            9.85        NaN           1          0          9.85        0.0
        """
        _check(self, actual, expected)


class Test_align_ccxt_orders_and_fills(hunitest.TestCase):
    """
    Verify that CCXT orders and fills are aligned correctly.
    """

    def test1(self) -> None:
        child_order_response_df = _get_child_order_response_df()
        fills_df = _get_fills_df()
        filled_df, unfilled_df = occforre.align_ccxt_orders_and_fills(
            child_order_response_df, fills_df
        )
        # Check filled df.
        expected = r"""
                order_type time_in_force  post_only  reduce_only  side  order_price stop_price  order_amount          order_update_timestamp          order_update_datetime                  first_timestamp                   last_timestamp                   first_datetime                    last_datetime     symbol    asset_id  buy_count  sell_count  taker_count  maker_count  price  amount   cost  transaction_cost  realized_pnl
        order
        7954906695      limit           GTC      False        False   buy         8.20       None            10  2023-03-15 16:35:41.205000+00:00 2023-03-15 16:35:41.205000+00:00 2023-03-31 16:35:39.268000+00:00 2023-03-31 16:35:39.268000+00:00 2023-03-31 16:35:39.268000+00:00 2023-03-31 16:35:39.268000+00:00   APE/USDT  6051632686          1           0            1            0   4.20      10  42.00          0.000009             0
        4551306695      limit           GTC      False        False  sell         9.85       None             1  2023-03-15 16:35:42.205000+00:00 2023-03-15 16:35:42.205000+00:00 2023-03-31 16:35:40.268000+00:00 2023-03-31 16:35:40.268000+00:00 2023-03-31 16:35:40.268000+00:00 2023-03-31 16:35:40.268000+00:00  AVAX/USDT  8717633868          1           0            0            1   5.85       1   5.85          0.000010             0
        """
        _check(self, filled_df, expected)
        # Check unfilled df.
        expected = r"""
                order_type time_in_force  post_only  reduce_only  side  order_price stop_price  order_amount          order_update_timestamp           order_update_datetime
        order
        3154906699      limit           GTC      False        False   buy         10.1       None             3  2023-03-15 16:35:44.205000+00:00  2023-03-15 16:35:44.205000+00:00
        7909906645      limit           GTC      False        False  sell          2.7       None             0  2023-03-15 16:35:43.205000+00:00  2023-03-15 16:35:43.205000+00:00
        """
        _check(self, unfilled_df, expected)


class Test_compute_filled_order_execution_quality(hunitest.TestCase):
    """
    Verify that filled order execution quality is computed correctly.
    """

    @staticmethod
    def get_fills_df() -> pd.DataFrame:
        """
        Generate synthetic data.
        """
        data = {
            "side": ["buy", "sell"],
            "order_price": [34.12, 15.805],
            "order_amount": [10.0, 7.0],
            "price": [4.2, 30.85],
            "amount": [5, 1],
        }
        idx = [7954906695, 14412582631]
        df = pd.DataFrame(data, index=idx)
        return df

    def test1(self) -> None:
        fills_df = self.get_fills_df()
        tick_decimals = 2
        actual = occforre.compute_filled_order_execution_quality(
            fills_df, tick_decimals
        )
        expected = r"""
                    direction  price_improvement_notional  price_improvement_bps  underfill_quantity  underfill_pct  underfill_notional_at_limit_price  underfill_notional_at_transaction_price
        7954906695           1                       29.92            8769.050410                 5.0       0.500000                             170.60                                     21.0
        14412582631         -1                       15.05            9522.303069                 6.0       0.857143                              94.83                                    185.1
        """
        _check(self, actual, expected)


class Test_aggregate_child_limit_orders_by_bar(hunitest.TestCase):
    """
    Test that internal child orders are summarized by bar and instrument
    correctly.
    """

    def test1(self) -> None:
        df = _get_child_order_df()
        freq = "T"
        actual = occforre.aggregate_child_limit_orders_by_bar(df, freq)
        expected = r"""
                                order_count            buy_order_count buy_limit_twap buy_limit_vwap buy_amount buy_notional sell_order_count sell_limit_twap sell_limit_vwap sell_amount sell_notional
        asset_id                   6051632686 8717633868      6051632686     6051632686     6051632686 6051632686   6051632686       8717633868      8717633868      8717633868  8717633868    8717633868
        bar_end_datetime
        2023-03-15 16:36:00+00:00         2        2               2       8.500225       8.500225       20.0     170.0045                2         13.3525         13.3525         2.0     26.704999
        """
        _check(self, actual, expected)
