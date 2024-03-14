locals {
  # Create a map for instance names to their IDs
  instance_name_to_id = { for instance in aws_instance.EC2Instance : instance.tags["Name"] => instance.id }
}

resource "aws_instance" "EC2Instance" {
  count                       = length(var.ec2_configs)
  ami                         = var.ec2_configs[count.index].ami
  instance_type               = var.ec2_configs[count.index].instance_type
  key_name                    = aws_key_pair.EC2KeyPair[count.index].key_name
  availability_zone           = var.ec2_configs[count.index].availability_zone
  tenancy                     = var.ec2_configs[count.index].tenancy
  subnet_id                   = lookup(var.subnet_name_to_id, var.ec2_configs[count.index].subnet_id, "")
  ebs_optimized               = var.ec2_configs[count.index].ebs_optimized
  vpc_security_group_ids      = [for sg_name in var.ec2_configs[count.index].vpc_security_group_ids : lookup(var.sg_name_to_id, sg_name, "")]
  associate_public_ip_address = var.ec2_configs[count.index].associate_public_ip_address
  source_dest_check           = var.ec2_configs[count.index].source_dest_check
  private_ip                  = var.ec2_configs[count.index].private_ip
  iam_instance_profile        = var.ec2_configs[count.index].iam_instance_profile
  root_block_device {
    volume_size           = var.ec2_configs[count.index].volume_size
    volume_type           = var.ec2_configs[count.index].volume_type
    delete_on_termination = var.ec2_configs[count.index].delete_on_termination
  }
  tags = {
    Name       = var.ec2_configs[count.index].instance_name
    Created_by = "terraform"
  }
}

resource "aws_route" "EC2InstanceRoute" {
  count                  = length(var.instance_routes)
  destination_cidr_block = var.instance_routes[count.index].destination_cidr_block
  instance_id            = lookup(local.instance_name_to_id, var.instance_routes[count.index].instance_id, "")
  route_table_id         = lookup(var.rt_name_to_id, var.instance_routes[count.index].route_table_name, "")
}

resource "aws_key_pair" "EC2KeyPair" {
  count      = length(var.ec2_configs)
  key_name   = var.ec2_configs[count.index].key_pair_name
  public_key = tls_private_key.TLSKey[count.index].public_key_openssh
  tags = {
    Name       = var.ec2_configs[count.index].key_pair_name
    Created_by = "terraform"
  }
}

resource "tls_private_key" "TLSKey" {
  count     = length(var.ec2_configs)
  algorithm = "RSA"
  rsa_bits  = 4096
}

resource "local_file" "public_key" {
  count           = length(var.ec2_configs)
  content         = tls_private_key.TLSKey[count.index].public_key_openssh
  filename        = "${path.module}/${var.ec2_configs[count.index].key_pair_name}.pub"
  file_permission = "0600"
}

resource "local_file" "private_key" {
  count             = length(var.ec2_configs)
  sensitive_content = tls_private_key.TLSKey[count.index].private_key_pem
  filename          = "${path.module}/${var.ec2_configs[count.index].key_pair_name}.pem"
  file_permission   = "0600"
}
