# A real provider with dummy credentials: planning new resources makes no API call,
# and aws_iam_policy_document is rendered locally, so the policy can be asserted exactly.
provider "aws" {
  region                      = "eu-west-1"
  access_key                  = "not-used"
  secret_key                  = "not-used"
  skip_credentials_validation = true
  skip_requesting_account_id  = true
  skip_metadata_api_check     = true
}

variables {
  landing_bucket_arn = "arn:aws:s3:::acme-costlake-landing"
  lake_bucket_arn    = "arn:aws:s3:::acme-costlake-lake"
}

run "least_privilege_policy" {
  command = plan

  assert {
    condition = (
      jsondecode(output.policy_json).Statement[1].Action == "s3:GetObject" &&
      jsondecode(output.policy_json).Statement[1].Resource == "arn:aws:s3:::acme-costlake-landing/*"
    )
    error_message = "The landing bucket must be read-only."
  }
  assert {
    condition     = !strcontains(output.policy_json, "s3:*") && !strcontains(output.policy_json, "\"*\"")
    error_message = "No wildcard action or resource is allowed."
  }
  assert {
    condition     = jsondecode(aws_iam_role.pipeline.assume_role_policy).Statement[0].Principal.Service == "ecs-tasks.amazonaws.com"
    error_message = "Only the configured principal may assume the role."
  }
  assert {
    condition     = length(jsondecode(output.policy_json).Statement) == 3
    error_message = "No KMS statement without a KMS key."
  }
}

run "kms_statement_when_key_is_set" {
  command = plan
  variables {
    kms_key_arn = "arn:aws:kms:eu-west-1:111111111111:key/00000000-0000-0000-0000-000000000000"
  }
  assert {
    condition     = length(jsondecode(output.policy_json).Statement) == 4
    error_message = "A KMS key must add a kms statement."
  }
}

run "rejects_unknown_principal_type" {
  command = plan
  variables {
    trusted_principal_type = "Federated"
  }
  expect_failures = [var.trusted_principal_type]
}
