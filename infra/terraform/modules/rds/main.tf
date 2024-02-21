resource "aws_db_subnet_group" "RDSDBSubnetGroup" {
  description = var.rds_subnet_group_desc
  name        = var.rds_subnet_group_name
  subnet_ids  = [for subnet_name in var.rds_subnet_ids : lookup(var.subnet_name_to_id, subnet_name, "")]
}

resource "aws_db_instance" "RDSDBInstance" {
  identifier                          = var.db_identifier
  allocated_storage                   = var.db_allocated_storage
  instance_class                      = var.db_instance_class
  engine                              = var.db_engine
  username                            = var.db_username
  password                            = var.db_password
  backup_window                       = var.db_backup_window
  backup_retention_period             = var.db_backup_retention_period
  availability_zone                   = var.db_availability_zone
  maintenance_window                  = var.db_maintenance_window
  multi_az                            = var.db_multi_az
  engine_version                      = var.db_engine_version
  auto_minor_version_upgrade          = var.db_auto_minor_version_upgrade
  license_model                       = var.db_license_model
  iops                                = var.db_iops
  publicly_accessible                 = var.db_publicly_accessible
  storage_type                        = var.db_storage_type
  port                                = var.db_port
  storage_encrypted                   = var.db_storage_encrypted
  kms_key_id                          = var.db_kms_key_id
  copy_tags_to_snapshot               = var.db_copy_tags_to_snapshot
  monitoring_interval                 = var.db_monitoring_interval
  iam_database_authentication_enabled = var.db_iam_database_authentication_enabled
  deletion_protection                 = var.db_deletion_protection
  performance_insights_enabled        = var.performance_insights_enabled
  db_subnet_group_name                = aws_db_subnet_group.RDSDBSubnetGroup.name
  vpc_security_group_ids              = [for sg_name in var.rds_security_group_ids : lookup(var.sg_name_to_id, sg_name, "")]
  skip_final_snapshot                 = var.skip_final_snapshot
}
