mock_provider "aws" {}

variables {
  landing_bucket_name   = "acme-costlake-landing"
  landing_bucket_region = "eu-west-1"
}

run "parquet_overwrite_export_of_focus_table" {
  command = plan

  assert {
    condition     = startswith(output.query_statement, "SELECT AvailabilityZone, BilledCost") && endswith(output.query_statement, "FROM FOCUS_1_0_AWS")
    error_message = "Unexpected query statement."
  }
  assert {
    condition     = aws_bcmdataexports_export.focus.export[0].destination_configurations[0].s3_destination[0].s3_output_configurations[0].overwrite == "OVERWRITE_REPORT"
    error_message = "Exports must overwrite the period so each refresh is a complete delivery."
  }
  assert {
    condition     = aws_bcmdataexports_export.focus.export[0].destination_configurations[0].s3_destination[0].s3_prefix == "aws/focus"
    error_message = "The prefix must match the aws-focus source."
  }
}

run "required_columns_are_enforced" {
  command = plan
  variables {
    columns = ["BilledCost"]
  }
  expect_failures = [var.columns]
}
