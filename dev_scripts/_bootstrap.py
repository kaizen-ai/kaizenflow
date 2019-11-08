"""
Import as:

import bootstrap as boot

This module:
    - is used to modify the running PYTHONPATH in order to find //amp libraries
    - can only depend on standard python library functions
    - cannot depend from any of our libraries or library installed by conda
"""

import os
import sys


def bootstrap(rel_path_to_amp_helpers):
    """
    Tweak PYTHONPATH to pick up amp libraries while we are configuring amp,
    breaking the circular dependency.

    Same code for dev_scripts/_setenv_*.py and dev_scripts/install/create_conda.py

    # TODO(gp): It is not easy to share it as an import. Maybe we can just read
    # it from a file an eval it.
    """
    # Store the values before any modification, by making a copy out of
    # paranoia.
    _PATH = str(os.environ["PATH"]) if "PATH" in os.environ else ""
    _PYTHONPATH = (
        str(os.environ["PYTHONPATH"]) if "PYTHONPATH" in os.environ else ""
    )
    exec_name = os.path.abspath(sys.argv[0])
    amp_path = os.path.abspath(
        os.path.join(os.path.dirname(exec_name), rel_path_to_amp_helpers)
    )

    def _report_env():
        print("rel_path_to_amp_helpers=%s" % rel_path_to_amp_helpers)
        print("PATH=%s" % _PATH)
        print("PYTHONPATH=%s" % _PYTHONPATH)
        print("sys.argv[0]=%s" % sys.argv[0])
        print("exec_name=%s" % exec_name)
        print("amp_path=%s" % amp_path)

    # Check that `//amp/helpers` exists.
    helpers_path = os.path.join(amp_path, "helpers")
    if not os.path.exists(helpers_path):
        _report_env()
        raise RuntimeError("Can't find '%s'" % helpers_path)
    # Update path.
    # We can't update os.environ since the script is already running.
    sys.path.insert(0, amp_path)
    # Test the imports.
    try:
        # pylint: disable=import-outside-toplevel
        import helpers.dbg as dbg_test  # isort:skip # noqa: E402,F401

        print("* Boostrap successful *")
    except ImportError as e:
        print("* Bootstrap failed *")
        _report_env()
        raise e
