output "efs_id" {
  description = "The ID of the file system."
  value       = aws_efs_file_system.EFSFileSystem.id
}

output "efs_mount_target_ids" {
  description = "The IDs of the EFS mount targets."
  value       = aws_efs_mount_target.EFSMountTarget[*].id
}

output "efs_dns_name" {
  description = "The DNS name of the EFS file system."
  value       = aws_efs_file_system.EFSFileSystem.dns_name
}
