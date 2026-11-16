variable "landing_bucket_name" {
  description = "Bucket receiving billing exports."
  type        = string
  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$", var.landing_bucket_name))
    error_message = "Must be a valid S3 bucket name."
  }
}

variable "lake_bucket_name" {
  description = "Bucket holding bronze and silver objects."
  type        = string
  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$", var.lake_bucket_name))
    error_message = "Must be a valid S3 bucket name."
  }
}

variable "landing_retention_days" {
  description = "Days raw exports stay in landing. Bronze keeps its own copy, so this only bounds storage cost."
  type        = number
  default     = 90
  validation {
    condition     = var.landing_retention_days >= 7
    error_message = "Keep raw exports at least 7 days so that late processing can still read them."
  }
}

variable "noncurrent_version_retention_days" {
  description = "Days a replaced object version is kept in the lake bucket (AWS only)."
  type        = number
  default     = 30
}

variable "aws_hardening" {
  description = "Enable AWS-only controls: public access block, SSE, ownership controls, TLS-only policy, noncurrent version expiry. Disable for S3-compatible stores."
  type        = bool
  default     = true
}

variable "kms_key_arn" {
  description = "KMS key for SSE-KMS. Null uses SSE-S3 (AES256)."
  type        = string
  default     = null
}

variable "landing_additional_policy_json" {
  description = "Extra bucket policy statements for the landing bucket (e.g. the AWS export service)."
  type        = string
  default     = null
}

variable "force_destroy" {
  description = "Allow destroying non-empty buckets. Only for disposable environments."
  type        = bool
  default     = false
}

variable "tags" {
  description = "Tags applied to every bucket."
  type        = map(string)
  default     = {}
}
