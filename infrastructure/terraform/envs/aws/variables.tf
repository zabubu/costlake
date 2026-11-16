variable "region" {
  description = "Region of the buckets."
  type        = string
  default     = "eu-west-1"
}

variable "environment" {
  description = "Environment name used in resource names."
  type        = string
  default     = "prod"
}

variable "owner" {
  description = "Owner tag."
  type        = string
}

variable "landing_bucket_name" {
  description = "Globally unique landing bucket name."
  type        = string
}

variable "lake_bucket_name" {
  description = "Globally unique lake bucket name."
  type        = string
}

variable "landing_retention_days" {
  description = "Retention of raw exports in landing."
  type        = number
  default     = 90
}

variable "kms_key_arn" {
  description = "Optional KMS key for bucket encryption."
  type        = string
  default     = null
}

variable "trusted_principal_type" {
  description = "Principal type allowed to assume the pipeline role."
  type        = string
  default     = "Service"
}

variable "trusted_principals" {
  description = "Principals allowed to assume the pipeline role."
  type        = list(string)
  default     = ["ecs-tasks.amazonaws.com"]
}
