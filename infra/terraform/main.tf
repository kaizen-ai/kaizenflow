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
  subnet_route_table_associations = var.subnet_route_table_associations
}

module "ec2" {
  source = "./modules/ec2"

  ec2_configs       = var.ec2_configs
  instance_routes   = var.instance_routes
  subnet_name_to_id = module.vpc.subnet_name_to_id
  sg_name_to_id     = module.vpc.sg_name_to_id
  rt_name_to_id     = module.vpc.rt_name_to_id
}

module "rds" {
  source = "./modules/rds"

  rds_subnet_group_desc = var.rds_subnet_group_desc
  rds_subnet_group_name = var.rds_subnet_group_name
  rds_subnet_ids        = var.rds_subnet_ids
  rds_configs           = var.rds_configs
  db_username           = jsondecode(data.aws_secretsmanager_secret_version.db_credentials.secret_string)["rds_db_username"]
  db_password           = jsondecode(data.aws_secretsmanager_secret_version.db_credentials.secret_string)["rds_db_password"]
  sg_name_to_id         = module.vpc.sg_name_to_id
  subnet_name_to_id     = module.vpc.subnet_name_to_id
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
  source = "./modules/iam"

  iam_roles            = var.iam_roles
  iam_policies         = var.iam_policies
  iam_role_policies    = var.iam_role_policies
  iam_instance_profile = var.iam_instance_profile
  vpc_id               = module.vpc.vpc_id
  eip_public_ip        = module.vpc.eip_public_ip
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

module "eks" {
  source = "./modules/eks"

  depends_on = [module.vpc, module.iam]

  cluster_name                      = var.cluster_name
  eks_cluster_role                  = var.eks_cluster_role
  eks_cluster_security_group_ids    = var.eks_cluster_security_group_ids
  eks_cluster_subnet_ids            = var.eks_cluster_subnet_ids
  eks_cluster_version               = var.eks_cluster_version
  enabled_cluster_log_types         = var.enabled_cluster_log_types
  eks_endpoint_private_access       = var.eks_endpoint_private_access
  eks_endpoint_public_access        = var.eks_endpoint_public_access
  eks_node_group_security_group_ids = var.eks_node_group_security_group_ids
  tag_specifications                = var.tag_specifications
  eks_node_group_subnet_ids         = var.eks_node_group_subnet_ids
  eks_node_role                     = var.eks_node_role
  eks_public_access_cidrs           = var.eks_public_access_cidrs
  node_ami_type                     = var.node_ami_type
  node_capacity_type                = var.node_capacity_type
  node_desired_size                 = var.node_desired_size
  node_disk_size                    = var.node_disk_size
  node_group_name                   = var.node_group_name
  node_instance_types               = var.node_instance_types
  node_max_size                     = var.node_max_size
  node_max_unavailable              = var.node_max_unavailable
  node_min_size                     = var.node_min_size
  node_disk_delete_on_termination   = var.node_disk_delete_on_termination
  node_disk_device_mapping_name     = var.node_disk_device_mapping_name
  node_disk_iops                    = var.node_disk_iops
  node_disk_type                    = var.node_disk_type
  node_http_put_response_hop_limit  = var.node_http_put_response_hop_limit
  node_metadata_http_tokens         = var.node_metadata_http_tokens
  iam_role_to_arn                   = module.iam.iam_role_to_arn
  sg_name_to_id                     = module.vpc.sg_name_to_id
  subnet_name_to_id                 = module.vpc.subnet_name_to_id
}

module "client_vpn_endpoint" {
  source = "./modules/client_vpn_endpoint"

  endpoint_name         = var.endpoint_name
  transport_protocol    = var.transport_protocol
  vpc_id                = var.vpc_id
  self_service          = var.self_service
  endpoint_description  = var.endpoint_description
  enable_banner         = var.enable_banner
  cloudwatch_log_group  = var.cloudwatch_log_group
  cloudwatch_log_stream = var.cloudwatch_log_stream
  authorize_all_groups  = var.authorize_all_groups
  split_tunnel          = var.split_tunnel
  enable_connection_log = var.enable_connection_log
  enable_client_connect = var.enable_client_connect
  vpn_port              = var.vpn_port
  server_cert_arn       = var.server_cert_arn
  auth_type             = var.auth_type
  target_network_cidr   = var.target_network_cidr
  root_cert_chain_arn   = var.root_cert_chain_arn
  sec_group             = var.sec_group
  auth_rule_description = var.auth_rule_description
  endpoint_subnet_id    = var.endpoint_subnet_id
  endpoint_CIDR         = var.endpoint_CIDR
  sesh_timeout          = var.sesh_timeout

}