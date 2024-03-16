output "vpc_id" {
  description = "The ID of the VPC"
  value       = aws_vpc.EC2VPC.id
}

output "eip_public_ip" {
  description = "The Public IP of the EIP"
  value       = aws_eip.EC2EIP.public_ip
}

output "security_groups" {
  description = "The IDs of the security groups"
  value       = aws_security_group.EC2SecurityGroup
}

output "security_group_rule_ingress" {
  description = "The IDs of the created security group ingress rules."
  value       = [for i in aws_security_group_rule.EC2SecurityGroupIngress : i.id]
}

output "security_group_rule_egress" {
  description = "The IDs of the created security group egress rules."
  value       = [for i in aws_security_group_rule.EC2SecurityGroupEgress : i.id]
}

output "subnets" {
  description = "The IDs of the subnets"
  value       = aws_subnet.EC2Subnet
}

output "nat_gateway_id" {
  description = "The ID of the NAT Gateway"
  value       = aws_nat_gateway.EC2NatGateway.id
}

output "internet_gateway_id" {
  description = "The ID of the Internet Gateway"
  value       = aws_internet_gateway.EC2InternetGateway.id
}

output "route_tables" {
  description = "The IDs of the route tables"
  value       = [for i in aws_route_table.EC2RouteTable : i.id]
}

output "routes" {
  description = "The route table IDs and destination CIDR blocks of the routes"
  value       = [for i in aws_route.EC2NatGatewayRoute : "${i.route_table_id} ${i.destination_cidr_block}"]
}

output "subnet_route_table_associations" {
  description = "The subnet IDs and route table IDs of the subnet-route table associations"
  value       = [for i in aws_route_table_association.EC2SubnetRouteTableAssociation : "${i.subnet_id} ${i.route_table_id}"]
}

output "sg_name_to_id" {
  description = "Map of security group names to their IDs"
  value       = local.sg_name_to_id
}

output "subnet_name_to_id" {
  description = "Map of subnet names to their IDs"
  value       = local.subnet_name_to_id
}

output "rt_name_to_id" {
  description = "Map of route table names to their IDs"
  value       = local.rt_name_to_id
}
