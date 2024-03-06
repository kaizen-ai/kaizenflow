resource "aws_backup_vault" "EC2BackupVault" {
  for_each = var.backup_configs

  name          = each.value.target_vault_name
  force_destroy = each.value.force_destroy
}

resource "aws_backup_plan" "EC2Backup" {
  for_each = var.backup_configs

  name = each.value.name

  rule {
    rule_name         = each.value.rule_name
    target_vault_name = each.value.target_vault_name
    schedule          = each.value.schedule
    start_window      = each.value.start_window
    completion_window = each.value.completion_window

    lifecycle {
      cold_storage_after = each.value.cold_storage_after
      delete_after       = each.value.delete_after
    }
  }
  depends_on = [aws_backup_vault.EC2BackupVault]
}

resource "aws_backup_selection" "EC2BackupSelection" {
  for_each = var.backup_configs

  name         = each.value.selection_name
  plan_id      = aws_backup_plan.EC2Backup[each.key].id
  iam_role_arn = each.value.iam_role_arn

  # Check if resources list is provided
  resources = length(each.value.resources) > 0 && each.value.resources[0] != "" ? each.value.resources : null

  # Optionally using selection_tag
  dynamic "selection_tag" {
    for_each = length(each.value.selection_tags) > 0 ? [each.value.selection_tags] : []
    content {
      type  = "STRINGEQUALS"
      key   = selection_tag.value["key"]
      value = selection_tag.value["value"]
    }
  }
}
