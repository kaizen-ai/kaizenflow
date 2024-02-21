resource "aws_efs_file_system" "EFSFileSystem" {
  performance_mode = var.efs_performance_mode
  encrypted        = var.efs_encrypted
  kms_key_id       = var.efs_kms_key_id
  throughput_mode  = var.efs_throughput_mode

  tags = {
    Name = var.efs_name
  }
}

resource "aws_efs_mount_target" "EFSMountTarget" {
  count           = length(var.efs_mount_targets)
  file_system_id  = aws_efs_file_system.EFSFileSystem.id
  subnet_id       = lookup(var.subnet_name_to_id, var.efs_mount_targets[count.index].subnet_id, "")
  security_groups = [for sg in var.efs_mount_targets[count.index].security_groups : lookup(var.sg_name_to_id, sg, "")]
  ip_address      = var.efs_mount_targets[count.index].ip_address
}
