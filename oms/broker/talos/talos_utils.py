"""
Import as:

import oms.broker.talos.talos_utils as obtataut
"""
import logging
import uuid

_LOG = logging.getLogger(__name__)


def get_order_id() -> str:
    """
    Get an order ID in UUID4 format.
    """
    return str(uuid.uuid4())
