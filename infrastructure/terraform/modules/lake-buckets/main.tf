# Landing bucket (written by the cloud provider's billing export) and lake bucket
# (bronze/silver objects written by costlake). Works against AWS S3 and against
# S3-compatible stores (SeaweedFS locally): AWS-only controls are behind
# var.aws_hardening because S3-compatible servers do not implement those APIs.

locals {
  buckets = {
    landing = var.landing_bucket_name
    lake    = var.lake_bucket_name
  }
}

resource "aws_s3_bucket" "this" {
  for_each      = local.buckets
  bucket        = each.value
  force_destroy = var.force_destroy
  tags          = merge(var.tags, { "costlake:role" = each.key })
}

# Bronze is the audit trail and silver partitions are replaced on every restatement:
# versioning lets an accidental overwrite or delete be rolled back.
resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.this["lake"].id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "landing" {
  bucket = aws_s3_bucket.this["landing"].id

  rule {
    id     = "expire-raw-exports"
    status = "Enabled"
    filter {}
    expiration {
      days = var.landing_retention_days
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  count  = var.aws_hardening ? 1 : 0
  bucket = aws_s3_bucket.this["lake"].id

  rule {
    id     = "expire-replaced-versions"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = var.noncurrent_version_retention_days
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }

  depends_on = [aws_s3_bucket_versioning.lake]
}

resource "aws_s3_bucket_public_access_block" "this" {
  for_each                = var.aws_hardening ? local.buckets : {}
  bucket                  = aws_s3_bucket.this[each.key].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "this" {
  for_each = var.aws_hardening ? local.buckets : {}
  bucket   = aws_s3_bucket.this[each.key].id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  for_each = var.aws_hardening ? local.buckets : {}
  bucket   = aws_s3_bucket.this[each.key].id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = var.kms_key_arn == null ? "AES256" : "aws:kms"
      kms_master_key_id = var.kms_key_arn
    }
    bucket_key_enabled = var.kms_key_arn != null
  }
}

data "aws_iam_policy_document" "tls_only" {
  for_each = var.aws_hardening ? local.buckets : {}

  source_policy_documents = each.key == "landing" && var.landing_additional_policy_json != null ? [var.landing_additional_policy_json] : []

  statement {
    sid       = "DenyInsecureTransport"
    effect    = "Deny"
    actions   = ["s3:*"]
    resources = [aws_s3_bucket.this[each.key].arn, "${aws_s3_bucket.this[each.key].arn}/*"]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "this" {
  for_each = var.aws_hardening ? local.buckets : {}
  bucket   = aws_s3_bucket.this[each.key].id
  policy   = data.aws_iam_policy_document.tls_only[each.key].json

  depends_on = [aws_s3_bucket_public_access_block.this]
}
