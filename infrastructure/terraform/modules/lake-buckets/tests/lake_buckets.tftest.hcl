mock_provider "aws" {
  mock_data "aws_iam_policy_document" {
    defaults = {
      json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
    }
  }
}

variables {
  landing_bucket_name = "acme-costlake-landing"
  lake_bucket_name    = "acme-costlake-lake"
}

run "hardened_by_default" {
  command = plan

  assert {
    condition     = length(aws_s3_bucket_public_access_block.this) == 2
    error_message = "Both buckets must block public access."
  }
  assert {
    condition     = alltrue([for b in aws_s3_bucket_public_access_block.this : b.restrict_public_buckets])
    error_message = "Public buckets must be restricted."
  }
  assert {
    condition     = flatten([for r in aws_s3_bucket_server_side_encryption_configuration.this["lake"].rule : [for d in r.apply_server_side_encryption_by_default : d.sse_algorithm]]) == ["AES256"]
    error_message = "Without a KMS key, SSE-S3 is expected."
  }
  assert {
    condition     = aws_s3_bucket_versioning.lake.versioning_configuration[0].status == "Enabled"
    error_message = "The lake bucket must be versioned."
  }
  assert {
    condition     = aws_s3_bucket.this["landing"].force_destroy == false
    error_message = "Buckets must not be force-destroyable by default."
  }
}

run "kms_encryption" {
  command = plan
  variables {
    kms_key_arn = "arn:aws:kms:eu-west-1:111111111111:key/00000000-0000-0000-0000-000000000000"
  }
  assert {
    condition     = flatten([for r in aws_s3_bucket_server_side_encryption_configuration.this["landing"].rule : [for d in r.apply_server_side_encryption_by_default : d.sse_algorithm]]) == ["aws:kms"]
    error_message = "A KMS key must switch encryption to SSE-KMS."
  }
}

run "s3_compatible_store" {
  command = plan
  variables {
    aws_hardening = false
  }
  assert {
    condition     = length(aws_s3_bucket_policy.this) == 0 && length(aws_s3_bucket_lifecycle_configuration.lake) == 0
    error_message = "AWS-only controls must be skipped for S3-compatible stores."
  }
  assert {
    condition     = aws_s3_bucket_lifecycle_configuration.landing.rule[0].expiration[0].days == 90
    error_message = "Landing retention applies everywhere."
  }
}

run "rejects_short_retention" {
  command = plan
  variables {
    landing_retention_days = 1
  }
  expect_failures = [var.landing_retention_days]
}
