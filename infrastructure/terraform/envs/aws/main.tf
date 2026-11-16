# AWS environment: hardened buckets, the FOCUS data export and the pipeline role.
# Never applied by CI. See docs/deployment.md before running `terraform apply`:
# S3 storage and requests are billed (a few cents per month at this data volume);
# AWS Data Exports itself is free.

terraform {
  required_version = ">= 1.7"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 6.0, < 7.0"
    }
  }
  # Remote state: `terraform init -backend-config=backend.hcl` (see backend.hcl.example).
  backend "s3" {}
}

provider "aws" {
  region = var.region
  default_tags {
    tags = local.tags
  }
}

# BCM Data Exports is served from us-east-1 only.
provider "aws" {
  alias  = "billing"
  region = "us-east-1"
  default_tags {
    tags = local.tags
  }
}

locals {
  tags = {
    "costlake:env"   = var.environment
    "costlake:owner" = var.owner
    "managed-by"     = "terraform"
  }
}

module "focus_export" {
  source    = "../../modules/aws-focus-export"
  providers = { aws = aws.billing }

  export_name           = "costlake-focus-${var.environment}"
  landing_bucket_name   = var.landing_bucket_name
  landing_bucket_region = var.region
  prefix                = "aws/focus"

  # The bucket policy must authorise the export service before the export exists.
  depends_on = [module.lake_buckets]
}

module "lake_buckets" {
  source = "../../modules/lake-buckets"

  landing_bucket_name            = var.landing_bucket_name
  lake_bucket_name               = var.lake_bucket_name
  landing_retention_days         = var.landing_retention_days
  kms_key_arn                    = var.kms_key_arn
  landing_additional_policy_json = data.aws_iam_policy_document.export_delivery.json
}

# Lets the export service write under aws/focus/, only for exports of this account.
data "aws_caller_identity" "current" {}

data "aws_iam_policy_document" "export_delivery" {
  statement {
    sid       = "AllowBillingExportDelivery"
    effect    = "Allow"
    actions   = ["s3:PutObject", "s3:GetBucketPolicy"]
    resources = ["arn:aws:s3:::${var.landing_bucket_name}", "arn:aws:s3:::${var.landing_bucket_name}/aws/focus/*"]
    principals {
      type        = "Service"
      identifiers = ["bcm-data-exports.amazonaws.com", "billingreports.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
    condition {
      test     = "ArnLike"
      variable = "aws:SourceArn"
      values = [
        "arn:aws:bcm-data-exports:us-east-1:${data.aws_caller_identity.current.account_id}:export/*",
        "arn:aws:cur:us-east-1:${data.aws_caller_identity.current.account_id}:definition/*",
      ]
    }
  }
}

module "pipeline_access" {
  source = "../../modules/pipeline-access"

  role_name              = "costlake-pipeline-${var.environment}"
  trusted_principal_type = var.trusted_principal_type
  trusted_principals     = var.trusted_principals
  landing_bucket_arn     = module.lake_buckets.landing_bucket_arn
  lake_bucket_arn        = module.lake_buckets.lake_bucket_arn
  kms_key_arn            = var.kms_key_arn
}
