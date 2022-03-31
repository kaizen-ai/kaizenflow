"""
Import as:

import oms.oms_talos_utils as oomtauti
"""
import logging
import uuid

_LOG = logging.getLogger(__name__)


def get_order_id() -> str:
    """
    Get an order ID in UUID4 format.
    """
    return str(uuid.uuid4())
