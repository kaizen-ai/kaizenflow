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
  for_each = {
    for pair in flatten([for role, policies in var.iam_role_policies : [for policy in policies : {
      role_name   = role
      policy_name = policy
    }]]) :
    "${pair.role_name}.${pair.policy_name}" => pair
  }

  role       = aws_iam_role.IAMRole[each.value.role_name].name
  policy_arn = aws_iam_policy.IAMPolicy[each.value.policy_name].arn
}

resource "aws_iam_instance_profile" "IAMInstanceProfile" {
  for_each = var.iam_roles
  name     = each.key
  role     = aws_iam_role.IAMRole[each.key].name
}
