output "iam_role_arns" {
  description = "ARNs of the created IAM roles"
  value       = [for i in aws_iam_role.IAMRole : i.arn]
}

output "iam_policy_arns" {
  description = "ARNs of the created IAM policies"
  value       = [for i in aws_iam_policy.IAMPolicy : i.arn]
}
