#variable "region" {
#  description = "The AWS region."
#  default     = "eu-north-1"
#}

variable "endpoint_CIDR" {
  description = "IP address range, in CIDR notation, from which to assign client IP addresses"
  type = string
}

variable "endpoint_description" {
  description = "Description for the Client VPN endpoint"
  type = string
}

variable "dns_servers" {
  description = "DNS servers for the Client VPN endpoint, if any"
  type = list(string)
  default = [ null ]
}

variable "sec_group" {
  description = "Security group to apply to the endpoint"
  type = set(string)
}

variable "self_service" {
  description = "Enable or disable self service"
  type = string
}

variable "server_cert_arn" {
  description = "Server certificate ARN"
  type = string
}

variable "sesh_timeout" {
  description = "Session timeout in hours"
  type = number
}

variable "split_tunnel" {
  description = "Enable or disable split tunnel"
  type = bool
}

variable "endpoint_name" {
  description = "Name of the Client VPN endpoint"
  type = string
}

variable "transport_protocol" {
  description = "Transport protocol (TCP/UDP)"
  type = string
}

variable "vpc_id" {
  description = "VPC ID in which the endpoint will be created"
  type = string
}

variable "vpn_port" {
  description = "Port through which the connection will be established"
  type = number
}

variable "active_directory_id" {
  description = "Active Directory ID, if needed"
  type = string
  default = null
}

variable "root_cert_chain_arn" {
  description = "Root Certificate chain ARN"
  type = string
}

variable "saml_provider_arn" {
  description = "SAML provider ARN, if needed"
  type = string
  default = null
}

variable "self_service_saml_provider_arn" {
  description = "Self service SAML provider ARN, if needed"
  type = string
  default = null
}

variable "auth_type" {
  description = "Type of authentication when connecting to the VPN"
  type = string
}

variable "enable_client_connect" {
  description = "Whether or not to use client connect"
  type = bool
}

variable "lambda_function_arn" {
  description = "Lambda function ARN"
  type = string
  default = null
}

variable "banner_text" {
  description = "Banner text to show"
  type = string
  default = null
}

variable "enable_banner" {
  description = "Enable banner text"
  type = bool
}

variable "cloudwatch_log_group" {
  description = "Cloudwatch log group"
  type = string
}

variable "cloudwatch_log_stream" {
  description = "Cloudwatch log stream"
  type = string
}

variable "enable_connection_log" {
  description = "Enable connection log"
  type = bool
}


variable "endpoint_subnet_id" {
  description = "Endpoint subnet id"
  type = string
}

variable "access_group_id" {
  description = "Access group id"
  type = string
  default = null
}

variable "authorize_all_groups" {
  description = "Authorize all groups"
  type = bool
}

variable "auth_rule_description" {
  description = "Description of the authorization rule"
  type = string
}

variable "target_network_cidr" {
  description = "Destination CIDR range for the authorization rule"
  type = string
}