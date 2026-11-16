output "landing_bucket" {
  description = "Landing bucket name."
  value       = aws_s3_bucket.this["landing"].bucket
}

output "lake_bucket" {
  description = "Lake bucket name."
  value       = aws_s3_bucket.this["lake"].bucket
}

output "landing_bucket_arn" {
  description = "Landing bucket ARN."
  value       = aws_s3_bucket.this["landing"].arn
}

output "lake_bucket_arn" {
  description = "Lake bucket ARN."
  value       = aws_s3_bucket.this["lake"].arn
}
