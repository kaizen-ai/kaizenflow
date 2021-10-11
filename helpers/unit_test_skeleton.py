"""
Import as:

import helpers.unit_test_skeleton as hunteske
"""

import logging
from typing import Any, Dict, List, Tuple

import pytest

import helpers.unit_test as huntes


_LOG = logging.getLogger(__name__)


class Test_Example(huntes.TestCase):
    def test_example1(self) -> None:
        pass
