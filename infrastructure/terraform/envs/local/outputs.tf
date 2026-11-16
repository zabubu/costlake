output "landing_bucket" {
  description = "Landing bucket."
  value       = module.lake_buckets.landing_bucket
}

output "lake_bucket" {
  description = "Lake bucket."
  value       = module.lake_buckets.lake_bucket
}
