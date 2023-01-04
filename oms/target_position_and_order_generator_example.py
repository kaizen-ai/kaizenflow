"""
Import as:

import oms.target_position_and_order_generator_example otpaoge
"""

import asyncio
import logging
from typing import Optional

import oms.portfolio_example as oporexam
import oms.target_position_and_order_generator as otpaorge

_LOG = logging.getLogger(__name__)


def get_TargetPositionAndOrderGenerator_example1(
    event_loop: Optional[asyncio.AbstractEventLoop],
    market_data,
) -> otpaorge.TargetPositionAndOrderGenerator:
    portfolio = oporexam.get_DataFramePortfolio_example1(
        event_loop,
        market_data=market_data,
    )
    order_dict = {
        "order_type": "price@twap",
        "order_duration_in_mins": 5,
    }
    optimizer_dict = {
        "backend": "pomo",
        "params": {
            "style": "cross_sectional",
            "kwargs": {
                "bulk_frac_to_remove": 0.0,
                "bulk_fill_method": "zero",
                "target_gmv": 1e5,
            },
        },
    }
    share_quantization = "nearest_share"
    target_position_and_order_generator = (
        otpaorge.TargetPositionAndOrderGenerator(
            portfolio,
            order_dict,
            optimizer_dict,
            None,
            share_quantization,
        )
    )
    return target_position_and_order_generator


def get_TargetPositionAndOrderGenerator_example2(
    event_loop: Optional[asyncio.AbstractEventLoop],
    market_data,
) -> otpaorge.TargetPositionAndOrderGenerator:
    portfolio = oporexam.get_DataFramePortfolio_example1(
        event_loop,
        market_data=market_data,
    )
    order_dict = {
        "order_type": "price@twap",
        "order_duration_in_mins": 5,
    }
    optimizer_dict = {
        "backend": "batch_optimizer",
        "dollar_neutrality_penalty": 0.1,
        "volatility_penalty": 0.0,
        "relative_holding_penalty": 0.0,
        "relative_holding_max_frac_of_gmv": 0.5,
        "target_gmv": 1e5,
        "target_gmv_upper_bound_penalty": 0.0,
        "target_gmv_hard_upper_bound_multiple": 1.0,
        "turnover_penalty": 0.1,
    }
    share_quantization = "nearest_share"
    target_position_and_order_generator = (
        otpaorge.TargetPositionAndOrderGenerator(
            portfolio,
            order_dict,
            optimizer_dict,
            None,
            share_quantization,
        )
    )
    return target_position_and_order_generator
