"""
Import as:

import im_v2.kibot.metadata.config as imvkimeco
"""


import helpers.hs3 as hs3

# TODO(\*): Merge / reconcile S3_PREFIX in with im.kibot/data/config.py.
S3_BUCKET = hs3.get_bucket()
S3_PREFIX = f"s3://{S3_BUCKET}/data/kibot/metadata"

# TODO(amr): move common configs between data & metadata to
# `im.kibot.config`
ENDPOINT = "http://www.kibot.com/"
API_ENDPOINT = "http://api.kibot.com/"

TICKER_LISTS_SUB_DIR = "raw/ticker_lists"
ADJUSTMENTS_SUB_DIR = "raw/adjustments"
