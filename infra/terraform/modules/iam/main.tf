locals {
  # Create a map for IAM role names to their ARNs
  iam_role_to_arn = { for iam_role in aws_iam_role.IAMRole : iam_role.name => iam_role.arn }

  flattened_role_policies = merge([
    for role, policies in var.iam_role_policies : {
      for idx, policy in policies : "${role}.${idx}" => {
        role_name  = role,
        policy_arn = try(policy.arn, null) != null ? policy.arn : aws_iam_policy.IAMPolicy[policy.name].arn
      }
    }
  ]...)
}

resource "aws_iam_role" "IAMRole" {
  for_each           = var.iam_roles
  name               = each.key
  description        = each.value.description
  assume_role_policy = templatefile("${path.module}/templates/roles/${each.key}.json.tpl", {})
}

resource "aws_iam_policy" "IAMPolicy" {
  for_each    = var.iam_policies
  name        = each.key
  description = each.value.description
  policy      = templatefile("${path.module}/templates/policies/${each.key}.json.tpl", { vpc_id = var.vpc_id, eip_public_ip = var.eip_public_ip })
}

resource "aws_iam_role_policy_attachment" "IAMRolePolicy" {
  for_each = local.flattened_role_policies

  role       = aws_iam_role.IAMRole[each.value.role_name].name
  policy_arn = each.value.policy_arn
}

resource "aws_iam_instance_profile" "IAMInstanceProfile" {
  for_each = var.iam_instance_profile
  name     = each.key
  role     = aws_iam_role.IAMRole[each.key].name
  tags = {
    Name = each.value.tags
  }
}
