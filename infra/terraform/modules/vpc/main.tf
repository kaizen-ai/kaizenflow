locals {
  # Create a map of security group names to their IDs
  sg_name_to_id = { for sg in aws_security_group.EC2SecurityGroup : sg.name => sg.id }

  # Create a map for subnet names to their IDs
  subnet_name_to_id = { for subnet in aws_subnet.EC2Subnet : subnet.tags["Name"] => subnet.id }

  # Create a map for route table names to their IDs
  rt_name_to_id = { for rt in aws_route_table.EC2RouteTable : rt.tags["Name"] => rt.id }
}

resource "aws_vpc" "EC2VPC" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = var.enable_dns_support
  enable_dns_hostnames = var.enable_dns_hostnames
  instance_tenancy     = var.instance_tenancy
  tags                 = var.vpc_tags
}

resource "aws_subnet" "EC2Subnet" {
  count                   = length(var.subnet_availability_zones)
  availability_zone       = var.subnet_availability_zones[count.index]
  cidr_block              = var.subnet_cidr_blocks[count.index]
  vpc_id                  = aws_vpc.EC2VPC.id
  map_public_ip_on_launch = var.subnet_map_public_ip_on_launch
  tags                    = var.subnet_tags[count.index]
}

resource "aws_internet_gateway" "EC2InternetGateway" {
  vpc_id = aws_vpc.EC2VPC.id
  tags   = var.internet_gateway_tags
}

resource "aws_route_table" "EC2RouteTable" {
  count  = length(var.route_tables)
  vpc_id = aws_vpc.EC2VPC.id
  tags   = var.route_tables[count.index].tags
}

resource "aws_route_table_association" "EC2SubnetRouteTableAssociation" {
  count = length(var.subnet_route_table_associations)

  route_table_id = aws_route_table.EC2RouteTable[var.subnet_route_table_associations[count.index].route_table_index].id
  subnet_id      = aws_subnet.EC2Subnet[var.subnet_route_table_associations[count.index].subnet_index].id
}

resource "aws_eip" "EC2EIP" {
  vpc  = true
  tags = {
    Name = var.eip_name
  }
}

resource "aws_nat_gateway" "EC2NatGateway" {
  subnet_id = lookup(local.subnet_name_to_id, var.nat_gateway_subnet_id, "")
  tags = {
    Name = var.nat_gateway_name
  }
  allocation_id = aws_eip.EC2EIP.id
}

resource "aws_route" "EC2InternetGatewayRoute" {
  count                  = length(var.internet_gateway_routes)
  destination_cidr_block = var.internet_gateway_routes[count.index].destination_cidr_block
  gateway_id             = aws_internet_gateway.EC2InternetGateway.id
  route_table_id         = aws_route_table.EC2RouteTable[var.internet_gateway_routes[count.index].route_table_index].id
}

resource "aws_route" "EC2NatGatewayRoute" {
  count                  = length(var.nat_gateway_routes)
  destination_cidr_block = var.nat_gateway_routes[count.index].destination_cidr_block
  nat_gateway_id         = aws_nat_gateway.EC2NatGateway.id
  route_table_id         = aws_route_table.EC2RouteTable[var.nat_gateway_routes[count.index].route_table_index].id
}

resource "aws_security_group" "EC2SecurityGroup" {
  count       = length(var.security_groups)
  name        = var.security_groups[count.index].name
  description = var.security_groups[count.index].description
  vpc_id      = aws_vpc.EC2VPC.id
  tags = merge(
    var.security_groups[count.index].tags,
    { "Name" = var.security_groups[count.index].name }
  )
}

resource "aws_security_group_rule" "EC2SecurityGroupIngress" {
  depends_on = [aws_security_group.EC2SecurityGroup]
  for_each = {
    for idx_rule in flatten([for idx, sg in var.security_groups : [for rule_index, rule in sg.ingress : { idx = idx, rule = rule, rule_index = rule_index }]]) :
    format("%d-ingress-%d", idx_rule.idx, idx_rule.rule_index) => idx_rule
  }

  security_group_id        = aws_security_group.EC2SecurityGroup[each.value.idx].id
  type                     = "ingress"
  from_port                = try(each.value.rule.from_port, "0")
  to_port                  = try(each.value.rule.to_port, "0")
  protocol                 = try(each.value.rule.protocol, "-1")
  description              = try(each.value.rule.description, null)
  cidr_blocks              = try(each.value.rule.cidr_blocks, [])
  ipv6_cidr_blocks         = try(each.value.rule.ipv6_cidr_blocks, [])
  prefix_list_ids          = try(each.value.rule.prefix_list_ids, [])
  source_security_group_id = try(local.sg_name_to_id[each.value.rule.security_groups[0]], null)
}

resource "aws_security_group_rule" "EC2SecurityGroupEgress" {
  depends_on = [aws_security_group.EC2SecurityGroup]
  for_each = {
    for idx_rule in flatten([for idx, sg in var.security_groups : [for rule_index, rule in sg.egress : { idx = idx, rule = rule, rule_index = rule_index }]]) :
    format("%d-egress-%d", idx_rule.idx, idx_rule.rule_index) => idx_rule
  }

  security_group_id        = aws_security_group.EC2SecurityGroup[each.value.idx].id
  type                     = "egress"
  from_port                = try(each.value.rule.from_port, "0")
  to_port                  = try(each.value.rule.to_port, "0")
  protocol                 = try(each.value.rule.protocol, "-1")
  description              = try(each.value.rule.description, null)
  cidr_blocks              = try(each.value.rule.cidr_blocks, [])
  ipv6_cidr_blocks         = try(each.value.rule.ipv6_cidr_blocks, [])
  prefix_list_ids          = try(each.value.rule.prefix_list_ids, [])
  source_security_group_id = try(local.sg_name_to_id[each.value.rule.security_groups[0]], null)
}
