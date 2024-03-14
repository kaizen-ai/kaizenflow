resource "aws_db_subnet_group" "RDSDBSubnetGroup" {
  description = var.rds_subnet_group_desc
  name        = var.rds_subnet_group_name
  subnet_ids  = [for subnet_name in var.rds_subnet_ids : lookup(var.subnet_name_to_id, subnet_name, "")]
}

resource "aws_db_instance" "RDSDBInstance" {
  count                               = length(var.rds_configs)
  identifier                          = var.rds_configs[count.index].db_identifier
  allocated_storage                   = var.rds_configs[count.index].db_allocated_storage
  instance_class                      = var.rds_configs[count.index].db_instance_class
  engine                              = var.rds_configs[count.index].db_engine
  db_name                             = var.rds_configs[count.index].db_name
  username                            = var.db_username
  password                            = var.db_password
  backup_window                       = var.rds_configs[count.index].db_backup_window
  backup_retention_period             = var.rds_configs[count.index].db_backup_retention_period
  availability_zone                   = var.rds_configs[count.index].db_availability_zone
  maintenance_window                  = var.rds_configs[count.index].db_maintenance_window
  multi_az                            = var.rds_configs[count.index].db_multi_az
  engine_version                      = var.rds_configs[count.index].db_engine_version
  auto_minor_version_upgrade          = var.rds_configs[count.index].db_auto_minor_version_upgrade
  license_model                       = var.rds_configs[count.index].db_license_model
  iops                                = var.rds_configs[count.index].db_iops
  publicly_accessible                 = var.rds_configs[count.index].db_publicly_accessible
  storage_type                        = var.rds_configs[count.index].db_storage_type
  port                                = var.rds_configs[count.index].db_port
  storage_encrypted                   = var.rds_configs[count.index].db_storage_encrypted
  kms_key_id                          = var.rds_configs[count.index].db_kms_key_id
  copy_tags_to_snapshot               = var.rds_configs[count.index].db_copy_tags_to_snapshot
  monitoring_interval                 = var.rds_configs[count.index].db_monitoring_interval
  iam_database_authentication_enabled = var.rds_configs[count.index].db_iam_database_authentication_enabled
  deletion_protection                 = var.rds_configs[count.index].db_deletion_protection
  performance_insights_enabled        = var.rds_configs[count.index].performance_insights_enabled
  db_subnet_group_name                = aws_db_subnet_group.RDSDBSubnetGroup.name
  vpc_security_group_ids              = [for sg_name in var.rds_configs[count.index].rds_security_group_ids : lookup(var.sg_name_to_id, sg_name, "")]
  skip_final_snapshot                 = var.rds_configs[count.index].skip_final_snapshot
}
