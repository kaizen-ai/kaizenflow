"""
Import as:

import helpers.hsql as hsql
"""

import helpers.henv as henv

# The problem here is that part of the code base end up including `hsql` which
# requires `psycopg2` even though it's not called at run-time.
# To simplify the dependency management we include the code of `hsql` only if
# `psycopg2` is present. If not, we just create a stub for the needed type hints.
if henv.has_module("psycopg2"):
    from helpers.hsql_implementation import *  # isort:skip  # noqa: F401,F403 # pylint: disable=unused-import,unused-wildcard-import,wildcard-import

else:
    from typing import Any

    DbConnection = Any
