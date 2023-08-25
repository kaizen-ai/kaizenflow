import datetime
import os
from typing import Any

import numpy as np
import pandas as pd

import helpers.hdbg as hdbg
import helpers.hio as hio
import helpers.hpandas as hpandas
import helpers.hunit_test as hunitest
import oms.broker.ccxt.ccxt_logs_reader as obcclore


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
    actual = hpandas.df_to_str(actual, num_rows=None)
    self_.assert_equal(actual, expected, fuzzy_match=True)


class TestCcxtLogsReader1(hunitest.TestCase):
    def test_init1(self) -> None:
        """
        Verify that the CCXT logs reader v1 is initialized correctly.
        """
        target_dir = self.get_scratch_space()
        broker_version = "v1"
        _write_test_data(target_dir, broker_version)
        reader = obcclore.CcxtLogsReader(target_dir)
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
        reader = obcclore.CcxtLogsReader(target_dir)
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
        reader = obcclore.CcxtLogsReader(target_dir)
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
        reader = obcclore.CcxtLogsReader(target_dir)
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
        reader = obcclore.CcxtLogsReader(target_dir)
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
        reader = obcclore.CcxtLogsReader(target_dir)
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
        reader = obcclore.CcxtLogsReader(target_dir)
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
        reader = obcclore.CcxtLogsReader(target_dir)
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
        reader = obcclore.CcxtLogsReader(target_dir)
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
        reader = obcclore.CcxtLogsReader(target_dir)
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
        reader = obcclore.CcxtLogsReader(target_dir)
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
        reader = obcclore.CcxtLogsReader(target_dir)
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
        reader = obcclore.CcxtLogsReader(target_dir)
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
    ) -> obcclore.CcxtLogsReader:
        """
        Get `CcxtLogsReader` instance.
        """
        target_dir = os.path.join(self._base_dir_name, "rt_data")
        reader = obcclore.CcxtLogsReader(target_dir)
        return reader
