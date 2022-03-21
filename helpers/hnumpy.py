"""
Import as:

import helpers.hnumpy as hnumpy
"""

import contextlib
from typing import Iterator

import numpy as np


# From https://stackoverflow.com/questions/49555991
@contextlib.contextmanager
def random_seed_context(seed: int) -> Iterator:
    """
    Context manager to isolate a numpy random seed.
    """
    state = np.random.get_state()
    np.random.seed(seed)
    try:
        yield
    finally:
        np.random.set_state(state)
