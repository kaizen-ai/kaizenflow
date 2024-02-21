output "backup_vault_arn" {
  description = "The ARN of the Backup Vault"
  value       = [for i in aws_backup_vault.EC2BackupVault : i.id]
}

output "backup_plan_ids" {
  description = "The IDs of the backup plans."
  value       = [for i in aws_backup_plan.EC2Backup : i.id]
}

output "backup_selection_ids" {
  description = "The IDs of the backup selections."
  value       = [for i in aws_backup_selection.EC2BackupSelection : i.id]
}
