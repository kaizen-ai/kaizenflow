output "vpn_server_public_ip" {
  description = "Public IP address of the VPN server."
  value       = module.ec2.vpn_server_public_ip
}

output "rds_endpoint" {
  description = "RDS Instance Endpoint"
  value       = module.rds.rds_endpoint
}

output "efs_dns_name" {
  description = "The DNS name of the EFS file system."
  value       = module.efs.efs_dns_name
}
