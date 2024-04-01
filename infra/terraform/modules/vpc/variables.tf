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

variable "eip_name" {
  description = "The Name to assign to the EIP"
  type        = string
}

variable "vpc_id" {
  description = "The ID of the VPC"
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
