module "vpc" {
  source = "./modules/vpc"

  vpc_cidr                        = var.vpc_cidr
  enable_dns_support              = var.enable_dns_support
  enable_dns_hostnames            = var.enable_dns_hostnames
  instance_tenancy                = var.instance_tenancy
  vpc_tags                        = var.vpc_tags
  security_groups                 = var.security_groups
  subnet_availability_zones       = var.subnet_availability_zones
  subnet_cidr_blocks              = var.subnet_cidr_blocks
  subnet_map_public_ip_on_launch  = var.subnet_map_public_ip_on_launch
  subnet_tags                     = var.subnet_tags
  internet_gateway_tags           = var.internet_gateway_tags
  nat_gateway_subnet_id           = var.nat_gateway_subnet_id
  nat_gateway_name                = var.nat_gateway_name
  vpc_id                          = module.vpc.vpc_id
  route_tables                    = var.route_tables
  internet_gateway_routes         = var.internet_gateway_routes
  nat_gateway_routes              = var.nat_gateway_routes
  instance_routes                 = var.instance_routes
  subnet_route_table_associations = var.subnet_route_table_associations
  instance_name_to_id             = module.ec2.instance_name_to_id
}

module "ec2" {
  source = "./modules/ec2"

  ec2_configs       = var.ec2_configs
  subnet_name_to_id = module.vpc.subnet_name_to_id
  sg_name_to_id     = module.vpc.sg_name_to_id
}

module "rds" {
  source = "./modules/rds"

  rds_subnet_group_desc                  = var.rds_subnet_group_desc
  rds_subnet_group_name                  = var.rds_subnet_group_name
  rds_subnet_ids                         = var.rds_subnet_ids
  db_identifier                          = var.db_identifier
  db_allocated_storage                   = var.db_allocated_storage
  db_instance_class                      = var.db_instance_class
  db_engine                              = var.db_engine
  db_username                            = jsondecode(data.aws_secretsmanager_secret_version.db_credentials.secret_string)["rds_db_username"]
  db_password                            = jsondecode(data.aws_secretsmanager_secret_version.db_credentials.secret_string)["rds_db_password"]
  db_backup_window                       = var.db_backup_window
  db_backup_retention_period             = var.db_backup_retention_period
  db_availability_zone                   = var.db_availability_zone
  db_maintenance_window                  = var.db_maintenance_window
  db_multi_az                            = var.db_multi_az
  db_engine_version                      = var.db_engine_version
  db_auto_minor_version_upgrade          = var.db_auto_minor_version_upgrade
  db_license_model                       = var.db_license_model
  db_iops                                = var.db_iops
  db_publicly_accessible                 = var.db_publicly_accessible
  db_storage_type                        = var.db_storage_type
  db_port                                = var.db_port
  db_storage_encrypted                   = var.db_storage_encrypted
  db_kms_key_id                          = var.db_kms_key_id
  db_copy_tags_to_snapshot               = var.db_copy_tags_to_snapshot
  db_monitoring_interval                 = var.db_monitoring_interval
  db_iam_database_authentication_enabled = var.db_iam_database_authentication_enabled
  db_deletion_protection                 = var.db_deletion_protection
  performance_insights_enabled           = var.performance_insights_enabled
  rds_security_group_ids                 = var.rds_security_group_ids
  skip_final_snapshot                    = var.skip_final_snapshot
  sg_name_to_id                          = module.vpc.sg_name_to_id
  subnet_name_to_id                      = module.vpc.subnet_name_to_id
}

module "efs" {
  source = "./modules/efs"

  efs_performance_mode = var.efs_performance_mode
  efs_encrypted        = var.efs_encrypted
  efs_kms_key_id       = var.efs_kms_key_id
  efs_throughput_mode  = var.efs_throughput_mode
  efs_name             = var.efs_name
  efs_mount_targets    = var.efs_mount_targets
  subnet_name_to_id    = module.vpc.subnet_name_to_id
  sg_name_to_id        = module.vpc.sg_name_to_id
}

module "iam" {
  source            = "./modules/iam"
  iam_roles         = var.iam_roles
  iam_policies      = var.iam_policies
  iam_role_policies = var.iam_role_policies
  vpc_id            = module.vpc.vpc_id
  eip_public_ip     = module.vpc.eip_public_ip
}

module "aws_backup" {
  source = "./modules/aws_backup"

  backup_configs = var.backup_configs
}

module "s3" {
  source = "./modules/s3"

  buckets       = var.buckets
  vpc_id        = module.vpc.vpc_id
  eip_public_ip = module.vpc.eip_public_ip
}
