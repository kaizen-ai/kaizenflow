# DF1 = reading from S3 bucket for 24 hours
# DF2 = SELECT FROM over 24 hours

# Remove "ended_downloaded_at" and "knowledge_time"
# Make timestamp an index

# Comparison 1: compare TIMESTAMP column, which ones are missing; print.

# Comparison 2: take timestamps present in both, compare row-by-row

