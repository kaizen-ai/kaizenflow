"""
An implementation of broker class for CCXT.

Import as:

import oms.ccxt_broker as occxbrok
"""

import collections
import datetime
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

import ccxt
import pandas as pd

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hio as hio
import helpers.hs3 as hs3
import helpers.hsecrets as hsecret
import helpers.hsql as hsql
import im_v2.common.universe as ivcu
import oms

_LOG = logging.getLogger(__name__)


class CcxtBroker(oms.Broker):
    def __init__(
        self,
        exchange_id: str,
        universe_version: str,
        mode: str,
        portfolio_id: str,
        contract_type: str,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        """
        Constructor.

        :param exchange: name of the exchange to initialize the broker for
        :param universe_version: version of the universe to use
        :param mode: supported values: "test", "prod", if "test", launches the broker
         in sandbox environment (not supported for every exchange),
         if "prod" launches with production API.
        :param contract_type: "spot" or "futures"
        """
        hdbg.dassert_in(mode, ["prod", "test"])
        hdbg.dassert_in(contract_type, ["spot", "futures"])
        self._mode = mode
        self._exchange_id = exchange_id
        self._contract_type = contract_type
        self._exchange = self._log_into_exchange()
        self._assert_order_methods_presence()
        # Enable mapping back from asset ids when placing orders.
        self._asset_id_to_symbol_mapping = self._build_asset_id_to_symbol_mapping(
            universe_version
        )
        # Will be used to determine timestamp since when to fetch orders.
        self.last_order_execution_ts: Optional[pd.Timestamp] = None
        # TODO(Juraj): not sure how to generalize this coinbasepro-specific parameter.
        self._portfolio_id = portfolio_id

    def get_fills(self, sent_orders: List[oms.Order] = None) -> List[oms.Fill]:
        """
        Return list of fills from the last order execution.

        :param sent_orders: a list of orders submitted by Broker
        :return: a list of filled orders
        """
        fills: List[oms.Fill] = []
        order_symbols = set(
            [
                self._asset_id_to_symbol_mapping[order.asset_id]
                for order in sent_orders
            ]
        )
        if self.last_order_execution_ts:
            # Load orders for each given symbol.
            for symbol in order_symbols:
                _LOG.info("Inside get_fills")
                orders = self._exchange.fetch_orders(
                    since=hdateti.convert_timestamp_to_unix_epoch(
                        self.last_order_execution_ts,
                    ),
                    symbol=symbol,
                )
                # Select closed orders.
                for order in orders:
                    if order["status"] == "closed":
                        # Select order matching to CCXT exchange id.
                        filled_order = [
                            order
                            for sent_order in sent_orders
                            if sent_order.ccxt_id == order["id"]
                        ][0]
                        # Create a Fill object.
                        fill = oms.Fill(
                            filled_order,
                            hdateti.convert_unix_epoch_to_timestamp(
                                order["timestamp"]
                            ),
                            num_shares=order["amount"],
                            price=order["price"],
                        )
                        fills.append(fill)
        return fills

    def _assert_order_methods_presence(self) -> None:
        """
        Assert that the requested exchange supports all methods necessary to
        make placing/fetching orders possible.
        """
        methods = ["createOrder", "fetchClosedOrders"]
        abort = False
        for method in methods:
            if not self._exchange.has[method]:
                _LOG.error(
                    "Method %s is unsupported for %s.", method, self._exchange_id
                )
                abort = True
        if abort:
            raise ValueError(
                f"The {self._exchange_id} exchange is not fully supported for placing orders."
            )

    async def _submit_orders(
        self,
        orders: List[oms.Order],
        wall_clock_timestamp: pd.Timestamp,
        *,
        dry_run: bool,
    ) -> List[oms.Order]:
        """
        Submit orders.
        """
        self.last_order_execution_ts = pd.Timestamp.now()
        sent_orders: List[str] = []
        for order in orders:
            # TODO(Juraj): perform bunch of assertions for order attributes.
            symbol = self._asset_id_to_symbol_mapping[order.asset_id]
            side = "buy" if order.diff_num_shares > 0 else "sell"
            order_resp = self._exchange.createOrder(
                symbol=symbol,
                type=order.type_,
                side=side,
                amount=abs(order.diff_num_shares),
                # id = order.order_id,
                # id=order.order_id,
                # TODO(Juraj): maybe it is possible to somehow abstract this to a general behavior
                # but most likely the method will need to be overriden per each exchange
                # to accomodate endpoint specific behavior.
                params={
                    "portfolio_id": self._portfolio_id,
                    "client_oid": order.order_id,
                },
            )
            order.ccxt_id = order_resp["id"]
            sent_orders.append(order)
            _LOG.info(order_resp)
        return sent_orders

    def _build_asset_id_to_symbol_mapping(
        self, universe_version: str
    ) -> Dict[int, str]:
        """
        Build asset id to full symbol mapping.
        """
        # Get full symbol universe.
        # TODO(Danya): Change mode to "trade".
        full_symbol_universe = ivcu.get_vendor_universe(
            "CCXT", "trade", version=universe_version, as_full_symbol=True
        )
        # Filter symbols of the exchange corresponding to this instance.
        full_symbol_universe = list(
            filter(
                lambda s: s.startswith(self._exchange_id), full_symbol_universe
            )
        )
        # Build mapping.
        asset_id_to_full_symbol_mapping = (
            ivcu.build_numerical_to_string_id_mapping(full_symbol_universe)
        )
        # Change mapped values to be symbol only (more convevient when placing orders)
        asset_id_to_symbol_mapping = {
            id_: ivcu.parse_full_symbol(fs)[1].replace("_", "/")
            for id_, fs in asset_id_to_full_symbol_mapping.items()
        }
        return asset_id_to_symbol_mapping

    async def _wait_for_accepted_orders(
        self,
        file_name: str,
    ) -> None:
        _ = file_name

    def _log_into_exchange(self) -> ccxt.Exchange:
        """
        Log into coinbasepro and return the corresponding `ccxt.Exchange`
        object.
        """
        # Select credentials for provided exchange.
        if self._mode == "test":
            secrets_id = self._exchange_id + "_sandbox"
        else:
            secrets_id = self._exchange_id
        exchange_params = hsecret.get_secret(secrets_id)
        # Enable rate limit.
        exchange_params["rateLimit"] = True
        # Log into futures/spot market.
        if self._contract_type == "futures":
            exchange_params["options"] = {"defaultType": "future"}
        # Create a CCXT Exchange class object.
        ccxt_exchange = getattr(ccxt, self._exchange_id)
        exchange = ccxt_exchange(exchange_params)
        if self._mode == "test":
            exchange.set_sandbox_mode(True)
            _LOG.warning("Running in sandbox mode")
        hdbg.dassert(
            exchange.checkRequiredCredentials(),
            msg="Required credentials not passed",
        )
        return exchange


class CcxtDbBroker(oms.DatabaseBroker):
    def __init__(
        self,
        exchange_id: str,
        universe_version: str,
        mode: str,
        contract_type: str,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        """
        Constructor.

        :param exchange: name of the exchange to initialize the broker for
        :param universe_version: version of the universe to use
        :param mode: supported values: "test", "prod", if "test", launches the broker
         in sandbox environment (not supported for every exchange),
         if "prod" launches with production API.
        :param contract_type: "spot" or "futures"
        """
        hdbg.dassert_in(mode, ["prod", "test"])
        hdbg.dassert_in(contract_type, ["spot", "futures"])
        self._mode = mode
        self._exchange_id = exchange_id
        self._contract_type = contract_type
        self._exchange = self._log_into_exchange()
        self._assert_order_methods_presence()
        # Enable mapping back from asset ids when placing orders.
        self._asset_id_to_symbol_mapping = self._build_asset_id_to_symbol_mapping(
            universe_version
        )
        # Will be used to determine timestamp since when to fetch orders.
        self.last_order_execution_ts: Optional[pd.Timestamp] = None

    def _log_into_exchange(self) -> ccxt.Exchange:
        """
        Log into coinbasepro and return the corresponding `ccxt.Exchange`
        object.
        """
        # Select credentials for provided exchange.
        if self._mode == "test":
            secrets_id = self._exchange_id + "_sandbox"
        else:
            secrets_id = self._exchange_id
        exchange_params = hsecret.get_secret(secrets_id)
        # Enable rate limit.
        exchange_params["rateLimit"] = True
        # Log into futures/spot market.
        if self._contract_type == "futures":
            exchange_params["options"] = {"defaultType": "future"}
        # Create a CCXT Exchange class object.
        ccxt_exchange = getattr(ccxt, self._exchange_id)
        exchange = ccxt_exchange(exchange_params)
        if self._mode == "test":
            exchange.set_sandbox_mode(True)
            _LOG.warning("Running in sandbox mode")
        hdbg.dassert(
            exchange.checkRequiredCredentials(),
            msg="Required credentials not passed",
        )
        return exchange

    def _assert_order_methods_presence(self) -> None:
        """
        Assert that the requested exchange supports all methods necessary to
        make placing/fetching orders possible.
        """
        methods = ["createOrder", "fetchClosedOrders"]
        abort = False
        for method in methods:
            if not self._exchange.has[method]:
                _LOG.error(
                    "Method %s is unsupported for %s.", method, self._exchange_id
                )
                abort = True
        if abort:
            raise ValueError(
                f"The {self._exchange_id} exchange is not fully supported for placing orders."
            )

    def _build_asset_id_to_symbol_mapping(
        self, universe_version: str
    ) -> Dict[int, str]:
        """
        Build asset id to full symbol mapping.
        """
        # Get full symbol universe.
        full_symbol_universe = ivcu.get_vendor_universe(
            "CCXT", "trade", version=universe_version, as_full_symbol=True
        )
        # Filter symbols of the exchange corresponding to this instance.
        full_symbol_universe = list(
            filter(
                lambda s: s.startswith(self._exchange_id), full_symbol_universe
            )
        )
        # Build mapping.
        asset_id_to_full_symbol_mapping = (
            ivcu.build_numerical_to_string_id_mapping(full_symbol_universe)
        )
        # Change mapped values to be symbol only (more convevient when placing orders)
        asset_id_to_symbol_mapping = {
            id_: ivcu.parse_full_symbol(fs)[1].replace("_", "/")
            for id_, fs in asset_id_to_full_symbol_mapping.items()
        }
        return asset_id_to_symbol_mapping

    async def _submit_orders(
        self,
        orders: List[oms.Order],
        wall_clock_timestamp: pd.Timestamp,
        *,
        dry_run: bool = False,
    ) -> str:
        """
        Load orders into a submitted orders table.

        Same as abstract method.

        :param orders: orders to be submitted.
        :return: a `file_name` representing the id of the submitted order in the DB
        """
        # Add an order in the submitted orders table.
        submitted_order_id = self._get_next_submitted_order_id()
        list_of_orders: List[Tuple[str, Any]] = []
        s3_file_name = self._get_file_name(
            wall_clock_timestamp,
            submitted_order_id,
        )
        # Create a submitted orders row.
        list_of_orders.append(("filename", s3_file_name))
        timestamp_db = self._get_wall_clock_time()
        list_of_orders.append(("timestamp_db", timestamp_db))
        orders_as_txt = oms.orders_to_string(orders)
        list_of_orders.append(("orders_as_txt", orders_as_txt))
        row = pd.Series(collections.OrderedDict(list_of_orders))
        self._submissions[timestamp_db] = row
        # Write the row into the DB and save orders on S3.
        if dry_run:
            _LOG.warning("Not submitting orders because of dry_run")
        else:
            hsql.execute_insert_query(
                self._db_connection, row, self._submitted_orders_table_name
            )
            hs3.to_file(orders_as_txt, file_name=s3_file_name, aws_profile="ck")
            local_file_name = os.path.basename(s3_file_name)
            hio.to_file(local_file_name, orders_as_txt)
        return s3_file_name

    def _get_file_name(
        self,
        curr_timestamp: pd.Timestamp,
        order_id: int,
    ) -> str:
        """
        Get the S3 file name for orders.

        The file should look like:
        cryptokaizen-data-test/ccxt_db_broker_test/20220704000000/positions.0.20220704_190819.txt

        :param curr_timestamp: timestamp for the file name
        :param order_id: internal ID of the order
        :return: order file name 
        """
        # TODO(Danya): Change the dst_dir, use hs3 to write.
        dst_dir = "s3://cryptokaizen-data-test/ccxt_db_broker_test/"
        # E.g., "targets/YYYYMMDD000000/<filename>.txt"
        date_dir = curr_timestamp.strftime("%Y%m%d")
        date_dir += "0" * 6
        # Add a timestamp for readability.
        curr_timestamp_as_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"order_{order_id}.{curr_timestamp_as_str}.txt"
        file_name = os.path.join(dst_dir, date_dir, file_name)
        _LOG.debug("file_name=%s", file_name)
        return file_name
