variable "backend_bucket_name" {
  description = "The S3 bucket name for Terraform backend."
  default     = "cryptokaizen-airflow-backend-state"
}

variable "backend_dynamodb_table" {
  description = "The DynamoDB table name for Terraform backend locks."
  default     = "cryptokaizen_airflow_backend_lock"
}

variable "region" {
  description = "The AWS region."
  default     = "eu-north-1"
}

variable "profile" {
  description = "The AWS CLI profile."
  default     = "ck"
}

variable "force_destroy" {
  description = "Force Destroying the S3 bucket"
  default     = "true"
}
