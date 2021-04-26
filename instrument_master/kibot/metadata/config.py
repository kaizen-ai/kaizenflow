# TODO(\*): Merge / reconcile S3_PREFIX in with instrument_master.kibot/data/config.py.
S3_PREFIX = "s3://alphamatic-data/data/kibot/metadata"

# TODO(amr): move common configs between data & metadata to
# `instrument_master.kibot.config`
ENDPOINT = "http://www.kibot.com/"
API_ENDPOINT = "http://api.kibot.com/"

TICKER_LISTS_SUB_DIR = "raw/ticker_lists"
ADJUSTMENTS_SUB_DIR = "raw/adjustments"
