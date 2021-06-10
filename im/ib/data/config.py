import os

S3_BUCKET = os.environ['AM_S3_BUCKET']
S3_PREFIX = f"s3://{S3_BUCKET}/data/ib"
S3_METADATA_PREFIX = os.path.join(S3_PREFIX, "metadata")
