#!/usr/bin/env python

import os
import sys

# These imports are relative to the top of the repo.
import dev_scripts._bootstrap as boot
boot.bootstrap("../..")

import helpers.env as env  # isort:skip

if __name__ == "__main__":
    txt, failed = env.get_system_signature()
    print(txt)
    if failed:
        raise RuntimeError("Can't import some libraries")
