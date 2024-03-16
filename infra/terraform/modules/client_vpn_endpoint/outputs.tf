output "vpn_endpoint_id" {
  description = "The ID of the Client VPN Endpoint Connection."
  value       = aws_ec2_client_vpn_endpoint.vpn-endpoint-1.id
}

output "vpn_endpoint_arn" {
  description = "The ARN of the Client VPN Endpoint Connection."
  value       = aws_ec2_client_vpn_endpoint.vpn-endpoint-1.arn
}