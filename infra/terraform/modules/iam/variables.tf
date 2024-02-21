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
  type        = map(list(string))
  default     = {}
}

variable "vpc_id" {
  description = "The VPC ID to be used in IAM policies"
  type        = string
}

variable "eip_public_ip" {
  description = "The Public IP of the EIP to be used in IAM policies"
  type        = string
}
