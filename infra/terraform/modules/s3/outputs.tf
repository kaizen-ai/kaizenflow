output "bucket_arns" {
  value       = { for b in aws_s3_bucket.S3Bucket : b.bucket => b.arn }
  description = "A map of bucket names to their ARNs"
}

output "bucket_ids" {
  value       = { for b in aws_s3_bucket.S3Bucket : b.bucket => b.id }
  description = "A map of bucket names to their IDs"
}
