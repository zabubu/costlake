variable "role_name" {
  description = "Name of the IAM role."
  type        = string
  default     = "costlake-pipeline"
}

variable "trusted_principal_type" {
  description = "Principal type allowed to assume the role (Service or AWS)."
  type        = string
  default     = "Service"
  validation {
    condition     = contains(["Service", "AWS"], var.trusted_principal_type)
    error_message = "Must be Service or AWS."
  }
}

variable "trusted_principals" {
  description = "Principals allowed to assume the role, e.g. [\"ecs-tasks.amazonaws.com\"]."
  type        = list(string)
  default     = ["ecs-tasks.amazonaws.com"]
}

variable "landing_bucket_arn" {
  description = "ARN of the landing bucket."
  type        = string
}

variable "lake_bucket_arn" {
  description = "ARN of the lake bucket."
  type        = string
}

variable "kms_key_arn" {
  description = "KMS key used by the buckets, if any."
  type        = string
  default     = null
}

variable "tags" {
  description = "Tags for the role."
  type        = map(string)
  default     = {}
}
