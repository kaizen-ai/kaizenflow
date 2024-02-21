output "backend_bucket_arn" {
  value = aws_s3_bucket.backend_state.arn
}

output "dynamodb_table_arn" {
  value = aws_dynamodb_table.backend_lock.arn
}
