variable "s3_endpoint" {
  description = "S3 endpoint of the local object store."
  type        = string
  default     = "http://seaweedfs:8333"
}

variable "s3_admin_access_key" {
  description = "Admin access key of the local object store (TF_VAR_s3_admin_access_key)."
  type        = string
  sensitive   = true
}

variable "s3_admin_secret_key" {
  description = "Admin secret key of the local object store (TF_VAR_s3_admin_secret_key)."
  type        = string
  sensitive   = true
}

variable "landing_bucket_name" {
  description = "Landing bucket name."
  type        = string
  default     = "costlake-landing"
}

variable "lake_bucket_name" {
  description = "Lake bucket name."
  type        = string
  default     = "costlake-lake"
}
