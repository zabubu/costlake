# Plans the whole AWS environment offline (mocked provider, no backend, no credentials).
mock_provider "aws" {
  mock_data "aws_caller_identity" {
    defaults = { account_id = "111111111111" }
  }
  mock_data "aws_partition" {
    defaults = { partition = "aws" }
  }
  mock_data "aws_iam_policy_document" {
    defaults = { json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}" }
  }
}

mock_provider "aws" {
  alias = "billing"
  mock_data "aws_caller_identity" {
    defaults = { account_id = "111111111111" }
  }
  mock_data "aws_partition" {
    defaults = { partition = "aws" }
  }
  mock_data "aws_iam_policy_document" {
    defaults = { json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}" }
  }
}

variables {
  owner               = "finops"
  landing_bucket_name = "acme-costlake-landing-test"
  lake_bucket_name    = "acme-costlake-lake-test"
}

run "plans_every_component" {
  command = plan

  assert {
    condition     = module.lake_buckets.landing_bucket == "acme-costlake-landing-test"
    error_message = "Landing bucket not planned."
  }
}
