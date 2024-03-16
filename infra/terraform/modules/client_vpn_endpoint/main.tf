resource "aws_ec2_client_vpn_endpoint" "vpn-endpoint-1" {
  client_cidr_block      = var.endpoint_CIDR
  description            = var.endpoint_description
  dns_servers            = var.dns_servers
  security_group_ids     = var.sec_group
  self_service_portal    = var.self_service
  server_certificate_arn = var.server_cert_arn
  session_timeout_hours  = var.sesh_timeout
  split_tunnel           = var.split_tunnel
  tags = {
    Name = var.endpoint_name
  }
  tags_all = {
    Name = var.endpoint_name
  }
  transport_protocol = var.transport_protocol
  vpc_id             = var.vpc_id
  vpn_port           = var.vpn_port
  authentication_options {
    active_directory_id            = var.active_directory_id
    root_certificate_chain_arn     = var.root_cert_chain_arn
    saml_provider_arn              = var.saml_provider_arn
    self_service_saml_provider_arn = var.self_service_saml_provider_arn
    type                           = var.auth_type
  }
  client_connect_options {
    enabled             = var.enable_client_connect
    lambda_function_arn = var.lambda_function_arn
  }
  client_login_banner_options {
    banner_text = var.banner_text
    enabled     = var.enable_banner
  }
  connection_log_options {
    cloudwatch_log_group  = var.cloudwatch_log_group
    cloudwatch_log_stream = var.cloudwatch_log_stream
    enabled               = var.enable_connection_log
  }
}


resource "aws_ec2_client_vpn_network_association" "target_network_assoc" {
  client_vpn_endpoint_id = aws_ec2_client_vpn_endpoint.vpn-endpoint-1.id
  subnet_id              = var.endpoint_subnet_id
}


resource "aws_ec2_client_vpn_authorization_rule" "auth_rule_vpc" {
  access_group_id        = var.access_group_id
  authorize_all_groups   = var.authorize_all_groups
  client_vpn_endpoint_id = aws_ec2_client_vpn_endpoint.vpn-endpoint-1.id
  description            = var.auth_rule_description
  target_network_cidr    = var.target_network_cidr
}

