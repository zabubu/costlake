output "landing_bucket" {
  description = "Set COSTLAKE_LANDING_BUCKET to this value."
  value       = module.lake_buckets.landing_bucket
}

output "lake_bucket" {
  description = "Set COSTLAKE_LAKE_BUCKET to this value."
  value       = module.lake_buckets.lake_bucket
}

output "pipeline_role_arn" {
  description = "Role to run costlake with."
  value       = module.pipeline_access.role_arn
}

output "focus_export_arn" {
  description = "AWS Data Exports job."
  value       = module.focus_export.export_arn
}
