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

variable "instance_routes" {
  description = "List of EC2 Instance routes to create in the route table"
  type = list(object({
    destination_cidr_block = string
    instance_id            = string
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
  description = "Map of IAM roles and their corresponding policies"
  type        = map(any)
  default     = {}
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



# <--- ./data-sources.tf --->

variable "secret_arn" {
  description = "The ARN of the secret in AWS Secrets Manager containing the RDS credentials"
  type        = string
}
