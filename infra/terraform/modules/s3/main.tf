resource "aws_s3_bucket" "S3Bucket" {
  for_each = var.buckets

  bucket        = each.value.name
  acl           = each.value.acl
  force_destroy = each.value.force_destroy

  tags = {
    Name       = each.value.name
    Created_by = "terraform"
  }

  dynamic "lifecycle_rule" {
    for_each = each.value.lifecycle_rules
    content {
      id                                     = lifecycle_rule.value.id
      enabled                                = lifecycle_rule.value.enabled
      prefix                                 = lifecycle_rule.value.prefix
      tags                                   = lifecycle_rule.value.tags
      abort_incomplete_multipart_upload_days = lifecycle_rule.value.abort_incomplete_multipart_upload_days

      dynamic "expiration" {
        for_each = lifecycle_rule.value.expiration != null ? [lifecycle_rule.value.expiration] : []
        content {
          days = expiration.value.days
        }
      }

      dynamic "noncurrent_version_expiration" {
        for_each = lifecycle_rule.value.noncurrent_version_expiration != null ? [lifecycle_rule.value.noncurrent_version_expiration] : []
        content {
          days = noncurrent_version_expiration.value.days
        }
      }

      dynamic "noncurrent_version_transition" {
        for_each = lifecycle_rule.value.noncurrent_version_transition != null ? [lifecycle_rule.value.noncurrent_version_transition] : []
        content {
          days          = noncurrent_version_transition.value.days
          storage_class = noncurrent_version_transition.value.storage_class
        }
      }

      dynamic "transition" {
        for_each = lifecycle_rule.value.transition != null ? [lifecycle_rule.value.transition] : []
        content {
          days          = transition.value.days
          storage_class = transition.value.storage_class
        }
      }
    }
  }
}

resource "aws_s3_bucket_versioning" "S3BucketVersioning" {
  for_each = var.buckets

  bucket = aws_s3_bucket.S3Bucket[each.key].id
  versioning_configuration {
    status = each.value.versioning
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "S3BucketSSE" {
  for_each = var.buckets

  bucket = aws_s3_bucket.S3Bucket[each.key].id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_replication_configuration" "S3BucketReplication" {
  for_each = { for k, v in var.buckets : k => v if v.replication_configuration != null }

  bucket = aws_s3_bucket.S3Bucket[each.key].id
  role   = each.value.replication_configuration.role

  rule {
    id     = each.value.replication_configuration.rules.id
    status = each.value.replication_configuration.rules.status

    destination {
      bucket        = each.value.replication_configuration.rules.destination.bucket
      storage_class = each.value.replication_configuration.rules.destination.storage_class
    }
  }
  depends_on = [aws_s3_bucket_versioning.S3BucketVersioning]
}

resource "aws_s3_bucket_public_access_block" "S3BucketPublicAccessBlock" {
  for_each = { for k, v in var.buckets : k => v if v.public_access_block != null }

  bucket = aws_s3_bucket.S3Bucket[each.key].id

  block_public_acls       = each.value.public_access_block.block_public_acls
  block_public_policy     = each.value.public_access_block.block_public_policy
  ignore_public_acls      = each.value.public_access_block.ignore_public_acls
  restrict_public_buckets = each.value.public_access_block.restrict_public_buckets
}

resource "aws_s3_bucket_policy" "S3BucketPolicy" {
  for_each = { for k, v in var.buckets : k => v if v.policy != null }

  bucket = aws_s3_bucket.S3Bucket[each.key].id
  policy = templatefile("${path.module}/templates/policies/${each.value.policy}.json.tpl", { vpc_id = var.vpc_id, eip_public_ip = var.eip_public_ip, bucket_name = each.value.name })
}
