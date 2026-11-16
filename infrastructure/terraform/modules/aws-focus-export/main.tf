# AWS Data Exports job delivering the FOCUS table to the landing bucket as Parquet,
# overwritten on each refresh. costlake treats every refreshed
# .../BILLING_PERIOD=YYYY-MM/ folder as a new delivery and replaces the period.
#
# The BCM Data Exports API is global and served from us-east-1: configure the
# provider passed to this module accordingly. The landing bucket policy must allow
# bcm-data-exports.amazonaws.com to write under the prefix *before* the export is
# created (see envs/aws/main.tf).

locals {
  query = "SELECT ${join(", ", var.columns)} FROM ${var.focus_table}"
}

resource "aws_bcmdataexports_export" "focus" {
  export {
    name        = var.export_name
    description = "FOCUS billing data for costlake"

    data_query {
      query_statement = local.query
    }

    destination_configurations {
      s3_destination {
        s3_bucket = var.landing_bucket_name
        s3_prefix = var.prefix
        s3_region = var.landing_bucket_region

        s3_output_configurations {
          overwrite   = "OVERWRITE_REPORT"
          format      = "PARQUET"
          compression = "PARQUET"
          output_type = "CUSTOM"
        }
      }
    }

    refresh_cadence {
      frequency = "SYNCHRONOUS"
    }
  }

  tags = var.tags
}
