variable "rds_configs" {
  description = "List of configurations for RDS instances."
  type = list(object({
    db_identifier                          = string
    db_allocated_storage                   = number
    db_instance_class                      = string
    db_engine                              = string
    db_name                                = string
    db_backup_window                       = string
    db_backup_retention_period             = number
    db_availability_zone                   = string
    db_maintenance_window                  = string
    db_multi_az                            = bool
    db_engine_version                      = string
    db_auto_minor_version_upgrade          = bool
    db_license_model                       = string
    db_iops                                = number
    db_publicly_accessible                 = bool
    db_storage_type                        = string
    db_port                                = number
    db_storage_encrypted                   = bool
    db_kms_key_id                          = string
    db_copy_tags_to_snapshot               = bool
    db_monitoring_interval                 = number
    db_iam_database_authentication_enabled = bool
    db_deletion_protection                 = bool
    performance_insights_enabled           = bool
    rds_security_group_ids                 = list(string)
    skip_final_snapshot                    = bool
  }))
  default = []
}

variable "db_username" {
  description = "Username for the master DB user"
  type        = string
}

variable "db_password" {
  description = "Password for the master DB user"
  type        = string
}

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

variable "subnet_name_to_id" {
  description = "Mapping from subnet names to IDs"
  type        = map(string)
}

variable "sg_name_to_id" {
  description = "Mapping from security group names to IDs"
  type        = map(string)
}
