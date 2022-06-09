"""
An implementation of broker class for CCXT.

Import as:

import oms.ccxt_broker as occxbrok
"""

import logging
from typing import Any, Dict, List, Optional

import ccxt
import pandas as pd

import helpers.hdbg as hdbg
import helpers.hsecrets as hsecret
import im_v2.common.universe.full_symbol as imvcufusy
import im_v2.common.universe.universe as imvcounun
import im_v2.common.universe.universe_utils as imvcuunut
import oms.broker as ombroker
import oms.order as omorder

_LOG = logging.getLogger(__name__)


class CcxtBroker(ombroker.Broker):
    def __init__(
        self,
        exchange_id: str,
        universe_version: str,
        mode: str,
        portfolio_id: str,
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
        """
        hdbg.dassert_in(mode, ["prod", "test"])
        self._mode = mode
        self._exchange_id = exchange_id
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

    def get_fills(self) -> List[ombroker.Fill]:
        """
        Return list of fills from the last order execution.
        """
        fills: List[ombroker.Fill] = []
        if self.last_order_execution_ts:
            _LOG.info("Inside get_fills")
            orders = self._exchange.fetch_orders(
                since=self.last_order_execution_ts
            )
            for order in orders:
                if order["status"] == "closed":
                    # TODO(Danya): Transform filled order to a `Fill` object.
                    fills.append(order["id"])
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
        orders: List[omorder.Order],
        wall_clock_timestamp: pd.Timestamp,
        *,
        dry_run: bool,
    ) -> str:
        """
        Submit orders.
        """
        self.last_order_execution_ts = pd.Timestamp.now()
        for order in orders:
            # TODO(Juraj): perform bunch of assertions for order attributes.
            symbol = self._asset_id_to_symbol_mapping[order.asset_id]
            side = "buy" if order.diff_num_shares > 0 else "sell"
            order_resp = self._exchange.createOrder(
                symbol=symbol,
                type=order.type_,
                side=side,
                amount=order.diff_num_shares,
                # id=order.order_id,
                # TODO(Juraj): maybe it is possible to somehow abstract this to a general behavior
                # but most likely the method will need to be overriden per each exchange
                # to accomodate endpoint specific behavior.
                params={
                    "portfolio_id": self._portfolio_id,
                    "client_order_id": order.order_id,
                },
            )
            _LOG.info(order_resp)

    def _build_asset_id_to_symbol_mapping(
        self, universe_version: str
    ) -> Dict[int, str]:
        """
        Build asset id to full symbol mapping.
        """
        # Get full symbol universe.
        full_symbol_universe = imvcounun.get_vendor_universe(
            "CCXT", version=universe_version, as_full_symbol=True
        )
        # Filter symbols of the exchange corresponding to this instance.
        full_symbol_universe = list(
            filter(
                lambda s: s.startswith(self._exchange_id), full_symbol_universe
            )
        )
        # Build mapping.
        asset_id_to_full_symbol_mapping = (
            imvcuunut.build_numerical_to_string_id_mapping(full_symbol_universe)
        )
        # Change mapped values to be symbol only (more convevient when placing orders)
        asset_id_to_symbol_mapping = {
            id_: imvcufusy.parse_full_symbol(fs)[1].replace("_", "/")
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
        credentials = hsecret.get_secret(secrets_id)
        # Enable rate limit.
        credentials["rateLimit"] = True
        # Create a CCXT Exchange class object.
        ccxt_exchange = getattr(ccxt, self._exchange_id)
        exchange = ccxt_exchange(credentials)
        if self._mode == "test":
            exchange.set_sandbox_mode(True)
            _LOG.warning("Running in sandbox mode")
        hdbg.dassert(
            exchange.checkRequiredCredentials(),
            msg="Required credentials not passed",
        )
        return exchange
