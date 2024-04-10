locals {
  cluster_security_group_id = aws_eks_cluster.EKSCluster.vpc_config[0].cluster_security_group_id
}

resource "aws_eks_cluster" "EKSCluster" {
  name                      = var.cluster_name
  role_arn                  = lookup(var.iam_role_to_arn, var.eks_cluster_role, "")
  version                   = var.eks_cluster_version
  enabled_cluster_log_types = var.enabled_cluster_log_types

  vpc_config {
    subnet_ids              = [for subnet_name in var.eks_cluster_subnet_ids : lookup(var.subnet_name_to_id, subnet_name, "")]
    security_group_ids      = [for sg_name in var.eks_cluster_security_group_ids : lookup(var.sg_name_to_id, sg_name, "")]
    endpoint_private_access = var.eks_endpoint_private_access
    endpoint_public_access  = var.eks_endpoint_public_access
    public_access_cidrs     = var.eks_public_access_cidrs
  }

  tags = {
    Name = var.cluster_name
  }
}

resource "aws_eks_node_group" "EKSNodeGroup" {
  cluster_name    = aws_eks_cluster.EKSCluster.name
  node_group_name = var.node_group_name
  node_role_arn   = lookup(var.iam_role_to_arn, var.eks_node_role, "")
  subnet_ids      = [for subnet_name in var.eks_node_group_subnet_ids : lookup(var.subnet_name_to_id, subnet_name, "")]

  scaling_config {
    desired_size = var.node_desired_size
    max_size     = var.node_max_size
    min_size     = var.node_min_size
  }

  update_config {
    max_unavailable = var.node_max_unavailable
  }

  launch_template {
    id      = aws_launch_template.EKSNodeGroupLaunchTemplate.id
    version = aws_launch_template.EKSNodeGroupLaunchTemplate.latest_version
  }

  ami_type       = var.node_ami_type
  capacity_type  = var.node_capacity_type
  instance_types = var.node_instance_types

  tags = {
    Name = var.node_group_name
  }

  labels = {
    Name = var.node_group_name
  }
}

resource "aws_launch_template" "EKSNodeGroupLaunchTemplate" {
  name_prefix = "${var.cluster_name}-${var.node_group_name}"
  description = "Launch Template for EKS Node Group ${var.node_group_name}"
  vpc_security_group_ids = concat(
    [for sg_name in var.eks_node_group_security_group_ids : lookup(var.sg_name_to_id, sg_name, "")],
    [local.cluster_security_group_id]
  )
  key_name = aws_key_pair.NodeGroupKeyPair.key_name

  block_device_mappings {
    device_name = var.node_disk_device_mapping_name

    ebs {
      volume_size           = var.node_disk_size
      volume_type           = var.node_disk_type
      iops                  = var.node_disk_iops
      delete_on_termination = var.node_disk_delete_on_termination
    }
  }

  metadata_options {
    http_tokens                 = var.node_metadata_http_tokens
    http_put_response_hop_limit = var.node_http_put_response_hop_limit
  }

  dynamic "tag_specifications" {
    for_each = var.tag_specifications

    content {
      resource_type = tag_specifications.value["resource_type"]
      tags = {
        Name = var.node_group_name
      }
    }
  }
}

resource "aws_key_pair" "NodeGroupKeyPair" {
  key_name   = "${var.node_group_name}-keypair"
  public_key = tls_private_key.TLSKey.public_key_openssh
  tags = {
    Name       = var.node_group_name
    Created_by = "terraform"
  }
}

resource "tls_private_key" "TLSKey" {
  algorithm = "RSA"
  rsa_bits  = 4096
}

resource "local_file" "public_key" {
  content         = tls_private_key.TLSKey.public_key_openssh
  filename        = "${path.module}/${var.node_group_name}-keypair.pub"
  file_permission = "0600"
}

resource "local_file" "private_key" {
  sensitive_content = tls_private_key.TLSKey.private_key_pem
  filename          = "${path.module}/${var.node_group_name}-keypair.pem"
  file_permission   = "0600"
}
