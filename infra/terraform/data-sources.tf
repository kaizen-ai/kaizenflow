data "aws_secretsmanager_secret_version" "db_credentials" {
  secret_id = var.secret_arn
}
