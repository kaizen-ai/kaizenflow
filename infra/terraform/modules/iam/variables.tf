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

variable "vpc_id" {
  description = "The VPC ID to be used in IAM policies"
  type        = string
}

variable "eip_public_ip" {
  description = "The Public IP of the EIP to be used in IAM policies"
  type        = string
}
