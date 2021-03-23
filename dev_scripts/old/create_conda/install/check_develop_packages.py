#!/usr/bin/env python

import os
import sys

# Dir of the current executable.
_CURR_DIR = os.path.dirname(sys.argv[0])

# This script is `//amp/dev_scripts/install/check_develop_packages.py`, so we
# need to go up two levels to reach `//amp`.
_AMP_REL_PATH = "../.."
_AMP_PATH = os.path.abspath(os.path.join(_CURR_DIR, _AMP_REL_PATH))
assert os.path.exists(_AMP_PATH), "Can't find '%s'" % _AMP_PATH
sys.path.insert(0, _AMP_PATH)

# pylint: wrong-import-position
import dev_scripts._bootstrap as boot  # isort:skip

boot.bootstrap(_AMP_REL_PATH)

# pylint: wrong-import-position
import helpers.env as env  # isort:skip

if __name__ == "__main__":
    txt, failed = env.get_system_signature()
    print(txt)
    if failed:
        raise RuntimeError("Can't import some libraries")
