import helpers.henv as henv

# The problem here is that part of the code base end up including `hsql` which
# requires `psycopg2` even though it's not called at run-time.
# To simplify the dependency management we include the code of `hsql` only if
# `psycopg2` is present. If not, we just create a stub for the needed type hints.
if henv.has_module("psycopg2"):
    from helpers.hsql_implementation import *

else:
    from typing import Any

    DbConnection = Any
