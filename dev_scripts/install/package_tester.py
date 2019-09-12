#!/usr/bin/env python

import os
import sys


# Store the values before any modification, by making a copy (out of paranoia).
_PATH = str(os.environ["PATH"])
_PYTHONPATH = str(os.environ["PYTHONPATH"])


def _config_env():
    """
    Tweak PYTHONPATH to pick up amp.
    """
    exec_name = os.path.abspath(sys.argv[0])
    # This script is in dev_scripts/install, so we need to go two level up to
    # find "helpers".
    amp_path = os.path.abspath(os.path.join(os.path.dirname(exec_name), "..",
                                            ".."))
    # Check that helpers exists.
    helpers_path = os.path.join(amp_path, "helpers")
    assert os.path.exists(helpers_path), "Can't find '%s'" % helpers_path
    # Update path.
    # We can't update os.environ since the script is already running.
    sys.path.insert(0, amp_path)
    # Test the imports.
    try:
        pass
    except ImportError as e:
        print("PYTHONPATH=%s" % _PYTHONPATH)
        print("amp_path=%s" % amp_path)
        raise e


# We need to tweak the PYTHONPATH before importing.
_config_env()


import helpers.env as env     # isort:skip

if __name__ == "__main__":
    print(env.get_system_signature())
