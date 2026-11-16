# Least-privilege IAM role assumed by the costlake runtime (ECS task, EC2 instance
# or CI/OIDC role): read the landing bucket, read/write the lake bucket, nothing else.

data "aws_iam_policy_document" "assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = var.trusted_principal_type
      identifiers = var.trusted_principals
    }
  }
}

resource "aws_iam_role" "pipeline" {
  name                 = var.role_name
  assume_role_policy   = data.aws_iam_policy_document.assume.json
  max_session_duration = 3600
  tags                 = var.tags
}

data "aws_iam_policy_document" "access" {
  statement {
    sid       = "ListBuckets"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [var.landing_bucket_arn, var.lake_bucket_arn]
  }
  statement {
    sid       = "ReadLanding"
    actions   = ["s3:GetObject"]
    resources = ["${var.landing_bucket_arn}/*"]
  }
  statement {
    sid       = "ReadWriteLake"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${var.lake_bucket_arn}/*"]
  }
  dynamic "statement" {
    for_each = var.kms_key_arn == null ? [] : [var.kms_key_arn]
    content {
      sid       = "UseBucketKey"
      actions   = ["kms:Decrypt", "kms:GenerateDataKey"]
      resources = [statement.value]
    }
  }
}

resource "aws_iam_role_policy" "access" {
  name   = "costlake-lake-access"
  role   = aws_iam_role.pipeline.id
  policy = data.aws_iam_policy_document.access.json
}
