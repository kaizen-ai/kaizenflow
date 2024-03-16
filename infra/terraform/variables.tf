# <--- ./provider --->

variable "region" {
  description = "The AWS region."
  default     = "eu-north-1"
}

variable "profile" {
  description = "The AWS CLI profile."
  default     = "ck"
}



# <--- ./modules/ec2 --->

variable "ec2_configs" {
  description = "List of configurations for EC2 instances."
  type = list(object({
    availability_zone           = string
    private_ip                  = string
    volume_size                 = number
    volume_type                 = string
    ami                         = string
    instance_type               = string
    subnet_id                   = string
    vpc_security_group_ids      = list(string)
    associate_public_ip_address = bool
    tenancy                     = string
    ebs_optimized               = bool
    source_dest_check           = bool
    delete_on_termination       = bool
    instance_name               = string
    key_pair_name               = string
    iam_instance_profile        = string
  }))
  default = []
}

variable "instance_routes" {
  description = "List of EC2 Instance routes to create in the route table"
  type = list(object({
    destination_cidr_block = string
    instance_id            = string
    route_table_name       = string
  }))
  default = []
}



# <--- ./modules/vpc --->

variable "vpc_cidr" {
  description = "The CIDR block for the VPC"
  type        = string
}

variable "enable_dns_support" {
  description = "Should be true if you want to enable DNS support in the VPC"
  type        = bool
}

variable "enable_dns_hostnames" {
  description = "Should be true if you want to enable DNS hostnames in the VPC"
  type        = bool
}

variable "instance_tenancy" {
  description = "A tenancy option for instances launched into the VPC"
  type        = string
}

variable "vpc_tags" {
  description = "Metadata to assign to the VPC"
  type        = map(string)
  default     = {}
}

variable "internet_gateway_tags" {
  description = "Metadata to assign to the VPC"
  type        = map(string)
  default     = {}
}

variable "security_groups" {
  description = "List of security groups to create"
  type = list(object({
    name        = string
    description = optional(string)
    tags        = optional(map(string))
    ingress = list(object({
      from_port        = optional(number)
      to_port          = optional(number)
      protocol         = optional(string)
      description      = optional(string)
      cidr_blocks      = optional(list(string))
      ipv6_cidr_blocks = optional(list(string))
      prefix_list_ids  = optional(list(string))
      security_groups  = optional(list(string))
    }))
    egress = list(object({
      from_port        = optional(number)
      to_port          = optional(number)
      protocol         = optional(string)
      description      = optional(string)
      cidr_blocks      = optional(list(string))
      ipv6_cidr_blocks = optional(list(string))
      prefix_list_ids  = optional(list(string))
      security_groups  = optional(list(string))
    }))
  }))
  default = []
}

variable "subnet_availability_zones" {
  description = "List of availability zones in which to create subnets"
  type        = list(string)
  default     = []
}

variable "subnet_cidr_blocks" {
  description = "List of CIDR blocks for the subnets"
  type        = list(string)
  default     = []
}

variable "subnet_map_public_ip_on_launch" {
  description = "Whether instances that are launched in this subnet should be assigned a public IP address"
  type        = bool
}

variable "subnet_tags" {
  description = "List of tags to apply to the subnets"
  type        = list(map(string))
  default     = [{}]
}

variable "nat_gateway_subnet_id" {
  description = "The Subnet ID of the subnet in which to create the NAT Gateway"
  type        = string
}

variable "nat_gateway_name" {
  description = "The Name to assign to the NAT Gateway"
  type        = string
}

variable "route_tables" {
  description = "List of route tables to create"
  type = list(object({
    tags = map(string)
  }))
  default = []
}

variable "internet_gateway_routes" {
  description = "List of Internet Gateway routes to create in the route table"
  type = list(object({
    destination_cidr_block = string
    route_table_index      = number
  }))
  default = []
}

variable "nat_gateway_routes" {
  description = "List of NAT Gateway routes to create in the route table"
  type = list(object({
    destination_cidr_block = string
    route_table_index      = number
  }))
  default = []
}

variable "subnet_route_table_associations" {
  description = "Associations between subnets and route tables"
  type = list(object({
    subnet_index      = number
    route_table_index = number
  }))
  default = []
}



# <--- ./modules/rds --->

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



# <--- ./modules/efs --->

variable "efs_performance_mode" {
  description = "The performance mode for the file system."
  type        = string
  default     = "generalPurpose"
}

variable "efs_encrypted" {
  description = "If true, the disk will be encrypted."
  type        = bool
  default     = true
}

variable "efs_kms_key_id" {
  description = "The ARN for the KMS encryption key."
  type        = string
  default     = ""
}

variable "efs_throughput_mode" {
  description = "Throughput mode for the file system."
  type        = string
  default     = "bursting"
}

variable "efs_name" {
  description = "The name of the file system."
  type        = string
  default     = "default-efs"
}

variable "efs_mount_targets" {
  description = "List of EFS mount targets configurations."
  type = list(object({
    subnet_id       = string
    security_groups = list(string)
    ip_address      = string
  }))
}



# <--- ./modules/iam --->

variable "iam_roles" {
  description = "IAM roles to be created"
  type = map(object({
    description = string
  }))
  default = {}
}

variable "iam_policies" {
  description = "IAM policies to be created"
  type = map(object({
    description = string
  }))
  default = {}
}

variable "iam_role_policies" {
  description = "Map of IAM roles to their corresponding policies, allowing either policy names or ARNs"
  type = map(list(object({
    name = optional(string), // Policy name for internally managed policies
    arn  = optional(string)  // ARN for externally managed policies
  })))
  default = {}
}

variable "iam_instance_profile" {
  description = "IAM Instance Profiles to be created"
  type = map(object({
    tags = string
  }))
  default = {}
}



# <--- ./modules/aws_backup --->

variable "backup_configs" {
  description = "A map of backup configurations."
  type = map(object({
    name               = string
    rule_name          = string
    target_vault_name  = string
    schedule           = string
    start_window       = number
    completion_window  = number
    cold_storage_after = number
    delete_after       = string
    selection_name     = string
    iam_role_arn       = string
    resources          = list(string)
    selection_tags     = map(string)
    force_destroy      = bool
  }))
  default = {}
}



# <--- ./modules/s3 --->

variable "buckets" {
  description = "A map of the S3 bucket configurations"
  type = map(object({
    name          = string
    acl           = string
    force_destroy = bool
    versioning    = string
    policy        = optional(string)
    public_access_block = optional(object({
      block_public_acls       = bool
      block_public_policy     = bool
      ignore_public_acls      = bool
      restrict_public_buckets = bool
    }))
    lifecycle_rules = list(object({
      id                                     = string
      enabled                                = bool
      prefix                                 = optional(string)
      tags                                   = optional(map(string))
      abort_incomplete_multipart_upload_days = optional(number)
      expiration = optional(object({
        days = number
      }))
      noncurrent_version_expiration = optional(object({
        days = number
      }))
      noncurrent_version_transition = optional(object({
        days          = number
        storage_class = string
      }))
      transition = optional(object({
        days          = number
        storage_class = string
      }))
    }))
    replication_configuration = optional(object({
      role = string
      rules = object({
        id     = string
        status = string
        destination = object({
          bucket        = string
          storage_class = string
        })
      })
    }))
  }))
  default = {}
}



# <--- ./modules/eks --->

variable "cluster_name" {
  description = "Name of the EKS cluster"
  type        = string
}

variable "eks_cluster_version" {
  description = "Version of the EKS cluster"
  type        = string
}

variable "enabled_cluster_log_types" {
  description = "List of the desired control plane logging to enable."
  type        = list(string)
}

variable "eks_cluster_role" {
  description = "The ARN of the EKS cluster role"
  type        = string
}

variable "eks_cluster_subnet_ids" {
  description = "List of subnet IDs for the EKS cluster"
  type        = list(string)
}

variable "eks_cluster_security_group_ids" {
  description = "List of security group IDs for the EKS cluster"
  type        = list(string)
}

variable "eks_endpoint_private_access" {
  description = "Whether the private API server endpoint is enabled"
  type        = bool
}

variable "eks_endpoint_public_access" {
  description = "Whether the public API server endpoint is enabled"
  type        = bool
}

variable "eks_public_access_cidrs" {
  description = "List of CIDR blocks that can access the EKS public API server endpoint"
  type        = list(string)
}

variable "eks_node_group_security_group_ids" {
  description = "List of security group IDs for the EKS Node Group"
  type        = list(string)
}

variable "tag_specifications" {
  description = "Tag specifications for different resource types in the launch template"
  type = list(object({
    resource_type = string
  }))
  default = []
}

variable "node_group_name" {
  description = "Name of the EKS node group"
  type        = string
}

variable "eks_node_role" {
  description = "The ARN of the EKS Node group role"
  type        = string
}

variable "eks_node_group_subnet_ids" {
  description = "List of subnet IDs for the EKS Node group"
  type        = list(string)
}

variable "node_desired_size" {
  description = "Desired number of nodes in the node group"
  type        = number
}

variable "node_max_size" {
  description = "Maximum number of nodes in the node group"
  type        = number
}

variable "node_min_size" {
  description = "Minimum number of nodes in the node group"
  type        = number
}

variable "node_max_unavailable" {
  description = "Maximum number of unavailable nodes during node group update"
  type        = number
}

variable "node_ami_type" {
  description = "Type of Amazon Machine Image (AMI) associated with the EKS Node Group"
  type        = string
}

variable "node_capacity_type" {
  description = "Type of capacity associated with the EKS Node Group"
  type        = string
}

variable "node_disk_size" {
  description = "The size of the volume in gigabytes for nodes."
  type        = number
}

variable "node_disk_type" {
  description = "The volume type for nodes"
  type        = string
}

variable "node_disk_iops" {
  description = "The amount of provisioned IOPS."
  type        = number
}

variable "node_disk_delete_on_termination" {
  description = "Whether the volume should be destroyed on node termination."
  type        = bool
}

variable "node_disk_device_mapping_name" {
  description = "The name of the device to mount."
  type        = string
}

variable "node_metadata_http_tokens" {
  description = "Whether or not the metadata service requires session tokens (IMDSv2)."
  type        = string
}

variable "node_http_put_response_hop_limit" {
  description = "The desired HTTP PUT response hop limit for instance metadata requests."
  type        = number
}

variable "node_instance_types" {
  description = "List of instance types associated with the EKS Node Group"
  type        = list(string)
}



# <--- ./modules/client_vpn_endpoint --->

variable "endpoint_CIDR" {
  description = "IP address range, in CIDR notation, from which to assign client IP addresses"
  type        = string
}

variable "endpoint_description" {
  description = "Description for the Client VPN endpoint"
  type        = string
}

variable "dns_servers" {
  description = "DNS servers for the Client VPN endpoint, if any"
  type        = list(string)
  default     = [null]
}

variable "sec_group" {
  description = "Security group to apply to the endpoint"
  type        = set(string)
}

variable "self_service" {
  description = "Enable or disable self service"
  type        = string
}

variable "server_cert_arn" {
  description = "Server certificate ARN"
  type        = string
}

variable "sesh_timeout" {
  description = "Session timeout in hours"
  type        = number
}

variable "split_tunnel" {
  description = "Enable or disable split tunnel"
  type        = bool
}

variable "endpoint_name" {
  description = "Name of the Client VPN endpoint"
  type        = string
}

variable "transport_protocol" {
  description = "Transport protocol (TCP/UDP)"
  type        = string
}

variable "vpc_id" {
  description = "VPC ID in which the endpoint will be created"
  type        = string
}

variable "vpn_port" {
  description = "Port through which the connection will be established"
  type        = number
}

variable "active_directory_id" {
  description = "Active Directory ID, if needed"
  type        = string
  default     = null
}

variable "root_cert_chain_arn" {
  description = "Root Certificate chain ARN"
  type        = string
}

variable "saml_provider_arn" {
  description = "SAML provider ARN, if needed"
  type        = string
  default     = null
}

variable "self_service_saml_provider_arn" {
  description = "Self service SAML provider ARN, if needed"
  type        = string
  default     = null
}

variable "auth_type" {
  description = "Type of authentication when connecting to the VPN"
  type        = string
}

variable "enable_client_connect" {
  description = "Whether or not to use client connect"
  type        = bool
}

variable "lambda_function_arn" {
  description = "Lambda function ARN"
  type        = string
  default     = null
}

variable "banner_text" {
  description = "Banner text to show"
  type        = string
  default     = null
}

variable "enable_banner" {
  description = "Enable banner text"
  type        = bool
}

variable "cloudwatch_log_group" {
  description = "Cloudwatch log group"
  type        = string
}

variable "cloudwatch_log_stream" {
  description = "Cloudwatch log stream"
  type        = string
}

variable "enable_connection_log" {
  description = "Enable connection log"
  type        = bool
}


variable "endpoint_subnet_id" {
  description = "Endpoint subnet id"
  type        = string
}

variable "access_group_id" {
  description = "Access group id"
  type        = string
  default     = null
}

variable "authorize_all_groups" {
  description = "Authorize all groups"
  type        = bool
}

variable "auth_rule_description" {
  description = "Description of the authorization rule"
  type        = string
}

variable "target_network_cidr" {
  description = "Destination CIDR range for the authorization rule"
  type        = string
}



# <--- ./data-sources.tf --->

variable "secret_arn" {
  description = "The ARN of the secret in AWS Secrets Manager containing the RDS credentials"
  type        = string
}
