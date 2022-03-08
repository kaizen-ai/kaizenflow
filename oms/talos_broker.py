"""
An implementation of broker class for Talos API.

Import as:

import oms.talos_broker as otalbrok
"""

import datetime
import logging
import uuid
from typing import List, Any
import oms.broker as ombroker

_LOG = logging.getLogger(__name__)


class TalosBroker(ombroker.AbstractBroker):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

    def submit_orders(self):

        return file_name

    def _submit_orders(self):
        return None

    @staticmethod
    def create_order(exchanges: List[str], quantity: float, symbol: str, trading_currency: str,
                     order_type: str, strategy: str, price: float, side: float):
        """
        Create an order.
        """

        return order

    @staticmethod
    def get_order_id():
        """
        Get an order ID in UUID4 format.
        """
        return str(uuid.uuid4())

    @staticmethod
    def get_talos_current_utc_timestamp():
        """
        Return the current UTC timestamp in Talos-acceptable format.

        Example: 2019-10-20T15:00:00.000000Z
        """
        utc_datetime = datetime.datetime.utcnow().strftime(
            "%Y-%m-%dT%H:%M:%S.000000Z"
        )
        return utc_datetime
