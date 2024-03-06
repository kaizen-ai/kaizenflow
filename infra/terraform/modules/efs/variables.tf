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

variable "subnet_name_to_id" {
  description = "Map of subnet names to their IDs."
  type        = map(string)
}

variable "sg_name_to_id" {
  description = "Map of security group names to their IDs."
  type        = map(string)
}
