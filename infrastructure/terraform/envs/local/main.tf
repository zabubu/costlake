# Local environment: buckets on the SeaweedFS S3 endpoint of docker-compose.
# Applied by the `terraform` service (`make bootstrap`), state kept in a Docker volume.

terraform {
  required_version = ">= 1.7"
  # State lives in the terraform-state Docker volume:
  # terraform init -backend-config=path=/state/terraform.tfstate
  backend "local" {}
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 6.0, < 7.0"
    }
  }
}

provider "aws" {
  region                      = "us-east-1"
  access_key                  = var.s3_admin_access_key
  secret_key                  = var.s3_admin_secret_key
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_region_validation      = true
  skip_requesting_account_id  = true
  s3_use_path_style           = true

  endpoints {
    s3 = var.s3_endpoint
  }
}

module "lake_buckets" {
  source = "../../modules/lake-buckets"

  landing_bucket_name    = var.landing_bucket_name
  lake_bucket_name       = var.lake_bucket_name
  landing_retention_days = 90
  aws_hardening          = false # SeaweedFS does not implement these AWS APIs
  force_destroy          = true  # disposable local environment
  tags                   = { "costlake:env" = "local" }
}
