#!/bin/bash -xe
python dev_scripts/replace_text.py --old "self.assertRaises" --new "self.assertRaises"