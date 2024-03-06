output "instance_ids" {
  description = "IDs of the EC2 instances."
  value       = aws_instance.EC2Instance.*.id
}

output "key_pair_names" {
  description = "Names of the EC2 key pairs."
  value       = aws_key_pair.EC2KeyPair.*.key_name
}

output "public_key_paths" {
  description = "Paths to the public keys for each key pair."
  value       = local_file.public_key.*.filename
}

output "private_key_paths" {
  description = "Paths to the private keys for each key pair."
  value       = local_file.private_key.*.filename
}

output "instance_name_to_id" {
  description = "Map of instance names to their IDs"
  value       = local.instance_name_to_id
}

output "vpn_server_public_ip" {
  description = "Public IP address of the VPN server."
  value       = { for instance in aws_instance.EC2Instance : instance.tags["Name"] => instance.public_ip if instance.tags["Name"] == "VPN-terraform" }
}
