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

variable "subnet_name_to_id" {
  description = "Mapping from subnet names to IDs"
  type        = map(string)
}

variable "sg_name_to_id" {
  description = "Mapping from security group names to IDs"
  type        = map(string)
}

variable "iam_role_to_arn" {
  description = "Mapping from IAM role names to their ARNs"
  type        = map(string)
}
