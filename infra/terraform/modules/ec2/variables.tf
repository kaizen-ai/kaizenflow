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

variable "subnet_name_to_id" {
  description = "Mapping from subnet names to IDs"
  type        = map(string)
}

variable "sg_name_to_id" {
  description = "Mapping from security group names to IDs"
  type        = map(string)
}

variable "rt_name_to_id" {
  description = "Mapping from route table names to IDs"
  type        = map(string)
}
