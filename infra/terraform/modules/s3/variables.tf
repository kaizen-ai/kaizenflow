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

variable "vpc_id" {
  description = "The VPC ID to be used in IAM policies"
  type        = string
}

variable "eip_public_ip" {
  description = "The Public IP of the EIP to be used in IAM policies"
  type        = string
}
