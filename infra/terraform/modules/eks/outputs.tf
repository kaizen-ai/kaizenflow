output "cluster_id" {
  description = "The EKS cluster ID"
  value       = aws_eks_cluster.EKSCluster.id
}

output "cluster_arn" {
  description = "The Amazon Resource Name (ARN) of the cluster"
  value       = aws_eks_cluster.EKSCluster.arn
}

output "cluster_endpoint" {
  description = "The endpoint for your EKS Kubernetes API"
  value       = aws_eks_cluster.EKSCluster.endpoint
}

output "cluster_identity_oidc_issuer" {
  description = "The OIDC issuer URL for the EKS cluster"
  value       = aws_eks_cluster.EKSCluster.identity.0.oidc.0.issuer
}

output "node_group_arn" {
  description = "The ARN of the node group"
  value       = aws_eks_node_group.EKSNodeGroup.arn
}

output "cluster_security_group_id" {
  description = "The security group ID that was created by Amazon EKS for the cluster."
  value       = aws_eks_cluster.EKSCluster.vpc_config[0].cluster_security_group_id
}
