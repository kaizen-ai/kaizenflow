variable "rds_subnet_group_desc" {
  description = "The description for the RDS subnet group"
  type        = string
}

variable "rds_subnet_group_name" {
  description = "The name of the RDS subnet group"
  type        = string
}

variable "rds_subnet_ids" {
  description = "List of subnet IDs to associate with the RDS DB"
  type        = list(string)
}

variable "db_identifier" {
  description = "The name of the RDS instance"
  type        = string
}

variable "db_allocated_storage" {
  description = "The allocated storage size for the RDS instance"
  type        = number
}

variable "db_instance_class" {
  description = "The instance type of the RDS instance"
  type        = string
}

variable "db_engine" {
  description = "The name of the database engine to be used for the RDS instance"
  type        = string
}

variable "db_username" {
  description = "Username for the master DB user"
  type        = string
}

variable "db_password" {
  description = "Password for the master DB user"
  type        = string
}

variable "db_backup_window" {
  description = "Preferred backup window for the RDS instance"
  type        = string
}

variable "db_backup_retention_period" {
  description = "Number of days to retain backups for"
  type        = number
}

variable "db_availability_zone" {
  description = "The AZ where the RDS instance will be created"
  type        = string
}

variable "db_maintenance_window" {
  description = "Preferred maintenance window for the RDS instance"
  type        = string
}

variable "db_multi_az" {
  description = "Specifies if the RDS instance is multi-AZ"
  type        = bool
}

variable "db_engine_version" {
  description = "The engine version to use"
  type        = string
}

variable "db_auto_minor_version_upgrade" {
  description = "Indicates that minor engine upgrades will be applied automatically to the DB instance during the maintenance window"
  type        = bool
}

variable "db_license_model" {
  description = "License model information for the RDS instance"
  type        = string
}

variable "db_iops" {
  description = "The amount of provisioned IOPS"
  type        = number
}

variable "db_publicly_accessible" {
  description = "Specifies the accessibility options for the DB instance"
  type        = bool
}

variable "db_storage_type" {
  description = "The storage type for the DB instance"
  type        = string
}

variable "db_port" {
  description = "The port on which the DB accepts connections"
  type        = number
}

variable "db_storage_encrypted" {
  description = "Specifies whether the DB instance is encrypted"
  type        = bool
}

variable "db_kms_key_id" {
  description = "The ARN for the KMS encryption key"
  type        = string
}

variable "db_copy_tags_to_snapshot" {
  description = "Specifies whether tags are copied to snapshots"
  type        = bool
}

variable "db_monitoring_interval" {
  description = "The interval, in seconds, between points when Enhanced Monitoring metrics are collected for the DB instance"
  type        = number
}

variable "db_iam_database_authentication_enabled" {
  description = "Specifies whether mapping of AWS Identity and Access Management (IAM) accounts to database accounts is enabled"
  type        = bool
}

variable "db_deletion_protection" {
  description = "Specifies whether the DB instance should have deletion protection enabled"
  type        = bool
}

variable "performance_insights_enabled" {
  description = "Specifies whether Performance Insights are enabled"
  type        = bool
}

variable "rds_security_group_ids" {
  description = "List of VPC security groups to associate"
  type        = list(string)
}

variable "skip_final_snapshot" {
  description = "Specifies whether the DB instance should skip the final snapshot"
  type        = bool
}

variable "subnet_name_to_id" {
  description = "Mapping from subnet names to IDs"
  type        = map(string)
}

variable "sg_name_to_id" {
  description = "Mapping from security group names to IDs"
  type        = map(string)
}
