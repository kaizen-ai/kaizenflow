"""
Import as:

import oms.ccxt.ccxt_broker_instances as occcbrin
"""
import logging

import pandas as pd

import dataflow_amp.system.Cx as dtfamsysc
import im_v2.ccxt.data.client as icdcl
import im_v2.common.data.client as icdc
import market_data as mdata
import oms.ccxt.ccxt_broker_v1 as occcbrv1
import oms.ccxt.ccxt_broker_v2 as occcbrv2
import oms.hsecrets as omssec
import oms.hsecrets.secret_identifier as ohsseide

_LOG = logging.getLogger(__name__)


def get_CcxtBroker_v1_prod_instance1(
    strategy_id: str,
    market_data: mdata.MarketData,
    universe_version: str,
    secret_identifier: omssec.SecretIdentifier,
    log_dir: str,
) -> occcbrv1.CcxtBroker_v1:
    """
    Build an `CcxtBroker_v1` for production.
    """
    exchange_id = secret_identifier.exchange_id
    stage = secret_identifier.stage
    account_type = secret_identifier.account_type
    contract_type = "futures"
    portfolio_id = "ccxt_portfolio_1"
    # Build ImClient.
    db_stage = "preprod"
    bid_ask_table = "ccxt_bid_ask_futures_raw"
    im_client = icdcl.get_CcxtSqlRealTimeImClient_example1(
        universe_version, db_stage, bid_ask_table
    )
    max_order_submit_retries = 1
    broker = occcbrv1.CcxtBroker_v1(
        exchange_id,
        account_type,
        portfolio_id,
        contract_type,
        secret_identifier,
        strategy_id=strategy_id,
        market_data=market_data,
        universe_version=universe_version,
        stage=stage,
        bid_ask_im_client=im_client,
        max_order_submit_retries=max_order_submit_retries,
        log_dir=log_dir,
    )
    return broker


def get_CcxtBroker_v2_prod_instance1(
    strategy_id: str,
    market_data: mdata.MarketData,
    universe_version: str,
    secret_identifier: omssec.SecretIdentifier,
    log_dir: str,
) -> occcbrv2.CcxtBroker_v2:
    """
    Build an `CcxtBroker` for production.
    """
    exchange_id = secret_identifier.exchange_id
    # exchange_id = "binance"
    stage = secret_identifier.stage
    # stage = "preprod"
    account_type = secret_identifier.account_type
    # account_type = "trading"
    contract_type = "futures"
    portfolio_id = "ccxt_portfolio_1"
    # Build ImClient.
    db_stage = "prod"
    bid_ask_table = "ccxt_bid_ask_futures_raw"
    im_client = icdcl.get_CcxtSqlRealTimeImClient_example1(
        universe_version, db_stage, bid_ask_table
    )
    max_order_submit_retries = 1
    broker = occcbrv2.CcxtBroker_v2(
        exchange_id,
        account_type,
        portfolio_id,
        contract_type,
        secret_identifier,
        strategy_id=strategy_id,
        market_data=market_data,
        universe_version=universe_version,
        stage=stage,
        bid_ask_im_client=im_client,
        max_order_submit_retries=max_order_submit_retries,
        log_dir=log_dir,
    )
    return broker


def get_CcxtBroker_v2(secret_id: int, log_dir: str) -> occcbrv2.CcxtBroker_v2:
    """
    Get a `CcxtBroker` instance.
    """
    # `MarketData` is not strictly needed to talk to exchange, but since it is
    #  required to init the `Broker` we pass something to make it work.
    asset_ids = None
    # TODO(Vlad): Get rid of the sticking to the prod instance.
    market_data = dtfamsysc.get_Cx_RealTimeMarketData_prod_instance1(asset_ids)
    universe_version = "v7.1"
    strategy_id = "C1b"
    exchange_id = "binance"
    stage = "preprod"
    account_type = "trading"
    secret_identifier = omssec.SecretIdentifier(
        exchange_id, stage, account_type, secret_id
    )
    ccxt_broker = get_CcxtBroker_v2_prod_instance1(
        strategy_id, market_data, universe_version, secret_identifier, log_dir
    )
    return ccxt_broker


# #############################################################################
# Example Instances
# #############################################################################


def get_CcxtBroker_example1(
    market_data: mdata.MarketData,
    exchange_id: str,
    contract_type: str,
    stage: str,
    secret_id: int,
) -> occcbrv1.CcxtBroker_v1:
    """
    Construct a CcxtBroker for connecting to the exchange inside scripts.

    :param exchange_id: name of exchange, e.g. "binance"
    :param contract_type: e.g. "futures"
    :param stage: e.g. "preprod"
    :param secret_id: e.g., 1
    :return: initialized CCXT broker
    """
    # Set default broker values.
    # TODO(Danya): Pass the universe as a parameter.
    # TODO(Danya): Build MarketData inside the function.
    universe = "v7.1"
    portfolio_id = "ccxt_portfolio_1"
    strategy_id = "C1b"
    account_type = "trading"
    secret_identifier = ohsseide.SecretIdentifier(
        exchange_id, stage, account_type, secret_id
    )
    # Initialize the broker.
    broker = occcbrv1.CcxtBroker_v1(
        exchange_id,
        account_type,
        portfolio_id,
        contract_type,
        secret_identifier,
        market_data=market_data,
        universe_version=universe,
        stage=stage,
        strategy_id=strategy_id,
    )
    return broker


# TODO(Dan): -> `get_MockCcxtRealTimeImClientMarketData`?
def get_RealTimeImClientMarketData_example2(
    im_client: icdc.RealTimeImClient,
) -> mdata.RealTimeMarketData2:
    """
    Create a RealTimeMarketData2 to use as placeholder in Broker.

    This example is geared to work with CcxtBroker.
    """
    asset_id_col = "asset_id"
    asset_ids = [1464553467]
    start_time_col_name = "start_timestamp"
    end_time_col_name = "end_timestamp"
    columns = None
    get_wall_clock_time = lambda: pd.Timestamp.now(tz="America/New_York")
    market_data = mdata.RealTimeMarketData2(
        im_client,
        asset_id_col,
        asset_ids,
        start_time_col_name,
        end_time_col_name,
        columns,
        get_wall_clock_time,
    )
    return market_data
